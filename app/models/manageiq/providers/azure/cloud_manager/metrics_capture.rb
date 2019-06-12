class ManageIQ::Providers::Azure::CloudManager::MetricsCapture < ManageIQ::Providers::BaseManager::MetricsCapture
  INTERVAL_1_MINUTE = "PT1M".freeze

  # Linux, Windows counters
  CPU_METERS     = ["\\Processor Information(_Total)\\% Processor Time",
                    "\\Processor(_Total)\\% Processor Time",
                    "\\Processor\\PercentProcessorTime",
                    "Percentage CPU"].freeze  

  NETWORK_METERS = ["\\NetworkInterface\\BytesTotal"].freeze # Linux (No windows counter available)

  MEMORY_METERS  = ["\\Memory\\PercentUsedMemory",
                    "\\Memory\\% Committed Bytes In Use",
                    "\\Memory\\Committed Bytes"].freeze

  DISK_METERS    = ["\\PhysicalDisk\\BytesPerSecond",
                    "\\PhysicalDisk(_Total)\\Disk Read Bytes/sec", # Windows
                    "\\PhysicalDisk(_Total)\\Disk Write Bytes/sec",
                    "\\LogicalDisk(_Total)\\Disk Read Bytes/sec",
                    "\\LogicalDisk(_Total)\\Disk Write Bytes/sec"].freeze # Windows

  COUNTER_INFO = [
    {
      :native_counters       => CPU_METERS,
      :calculation           => ->(stat) { stat },
      :vim_style_counter_key => "cpu_usage_rate_average"
    },
    {
      :native_counters       => NETWORK_METERS,
      :calculation           => ->(stat) { stat / 1.kilobyte },
      :vim_style_counter_key => "net_usage_rate_average",
    },
    {
      :native_counters       => MEMORY_METERS,
      :calculation           => ->(stat) { stat },
      :vim_style_counter_key => "mem_usage_absolute_average",
    },
    {
      :native_counters       => DISK_METERS,
      :calculation           => ->(stat) { stat.sum / 1.kilobyte },
      :vim_style_counter_key => "disk_usage_rate_average",
    }
  ].freeze

  COUNTER_NAMES = COUNTER_INFO.flat_map { |i| i[:native_counters] }.uniq.to_set.freeze

  VIM_STYLE_COUNTERS = {
    "cpu_usage_rate_average"     => {
      :counter_key           => "cpu_usage_rate_average",
      :instance              => "",
      :capture_interval      => "20",
      :precision             => 1,
      :rollup                => "average",
      :unit_key              => "percent",
      :capture_interval_name => "realtime"
    },
    "mem_usage_absolute_average" => {
      :counter_key           => "mem_usage_absolute_average",
      :instance              => "",
      :capture_interval      => "20",
      :precision             => 1,
      :rollup                => "average",
      :unit_key              => "percent",
      :capture_interval_name => "realtime"
    },
    "net_usage_rate_average"     => {
      :counter_key           => "net_usage_rate_average",
      :instance              => "",
      :capture_interval      => "20",
      :precision             => 1,
      :rollup                => "average",
      :unit_key              => "kilobytespersecond",
      :capture_interval_name => "realtime"
    },
    "disk_usage_rate_average"    => {
      :counter_key           => "disk_usage_rate_average",
      :instance              => "",
      :capture_interval      => "20",
      :precision             => 1,
      :rollup                => "average",
      :unit_key              => "kilobytespersecond",
      :capture_interval_name => "realtime"
    },
  }.freeze

  def perf_collect_metrics(interval_name, start_time = nil, end_time = nil)
    raise "No EMS defined" if target.ext_management_system.nil?

    log_header = "[#{interval_name}] for: [#{target.class.name}], [#{target.id}], [#{target.name}]"

    end_time   = (end_time || Time.now).utc
    start_time = (start_time || end_time - 4.hours).utc # 4 hours for symmetry with VIM

    begin
      # This is just for consistency, to produce a :connect benchmark
      Benchmark.realtime_block(:connect) {}
      target.ext_management_system.with_provider_connection do |conn|
        with_metrics_services(conn) do |metrics_conn|
          perf_capture_data_azure(metrics_conn, start_time, end_time)
        end
      end
    rescue Exception => err
      _log.error("#{log_header} Unhandled exception during perf data collection: [#{err}], class: [#{err.class}]")
      _log.error("#{log_header}   Timings at time of error: #{Benchmark.current_realtime.inspect}")
      _log.log_backtrace(err)
      raise
    end
  end

  private

  def with_metrics_services(connection)
    metrics_conn = Azure::Armrest::Insights::MetricsService.new(connection)
    yield metrics_conn
  end



  def perf_capture_data_azure(metrics_conn, start_time, end_time)
    start_time -= 1.minute # Get one extra minute so we can build the 20-second intermediate values
    counters                = get_counters(metrics_conn)

    
    counter_values_by_ts    = {}

    counters.each do |md|
      filter = "metricnames=#{md.name.value}"
      filter << "&timespan=#{start_time.iso8601.gsub(/\+[0-9]*:[0-9]*/, "Z")}/#{end_time.iso8601.gsub(/\+[0-9]*:[0-9]*/, "Z")}"
      filter << "&interval=#{INTERVAL_1_MINUTE}"
      metrics = metrics_conn.list_metrics(azure_vm_id,filter)

      COUNTER_INFO.each do |i|
        if i[:native_counters].flat_map.include?(md.name.value)
          metrics[0].timeseries[0].data.each do |mtd|
               value = i[:calculation].call(mtd["average"])
               ts = DateTime.parse(mtd["timeStamp"]).to_date
               last_ts = ts - 1.minute

               ## # For (temporary) symmetry with VIM API we create 20-second intervals. )
              (last_ts + 20.seconds..ts).step_value(20.seconds).each do |inner_ts|
                  counter_values_by_ts.store_path(inner_ts.iso8601, i[:vim_style_counter_key], value) if not value.nil?
              end
          end
        end
      end 
    end.uniq.compact.sort

    counters_by_id              = {target.ems_ref => VIM_STYLE_COUNTERS}
    counter_values_by_id_and_ts = {target.ems_ref => counter_values_by_ts}
    _log.info(counter_values_by_ts)

    return counters_by_id, counter_values_by_id_and_ts
  end

 ##Compose it to be azure vm id 
  def azure_vm_id
    ems_ref_parts = target.ems_ref.split('/')
    vm_id = File.join("subscriptions",
      ems_ref_parts[0],
      "resourceGroups",
      ems_ref_parts[1],
      "providers",
      ems_ref_parts[2],
      "virtualMachines",
      ems_ref_parts[4])
      return vm_id
  end

  def get_counters(metrics_conn)
    ems = target.ext_management_system

    unless ems.insights?
      _log.info("Metrics not supported for region: " + "[#{ems.provider_region}]")
      return []
    end

    begin
      counters, _timings = Benchmark.realtime_block(:capture_counters) do
        
       
        
        metrics_conn
          .list_definitions(azure_vm_id)
          .select { |m| COUNTER_NAMES.include?(m.name.value) }
      end
    rescue ::Azure::Armrest::BadRequestException # Probably means region is not supported
      msg = "Problem collecting metrics for #{target.name}/#{target.resource_group.name}. "\
            "Region [#{ems.provider_region}] may not be supported."
      _log.warn(msg)
      counters = []
    rescue ::Azure::Armrest::RequestTimeoutException # Problem on Azure side
      _log.warn("Timeout attempting to collect metrics definitions for: #{target.name}/#{target.resource_group.name}. Skipping.")
      counters = []
    rescue ::Azure::Armrest::NotFoundException # VM deleted
      _log.warn("Could not find metrics definitions for: #{target.name}/#{target.resource_group.name}. Skipping.")
      counters = []
    rescue Exception => err
      _log.error("Unhandled exception during counter collection: #{target.name}/#{target.resource_group.name}")
      _log.log_backtrace(err)
      raise
    end

    counters
  end
end
