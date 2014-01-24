#!/usr/bin/env ruby
# pmt - Process Management Tool
# This script mainly executes mpirun and watches it's output to make
# sure all the process are started and stopped.

require "xmlrpc/server"
require "open3"
require "tempfile"
require "logger"
require "optparse"
require "ostruct"
require "socket"

class LoggerIO
  # The standard ruby logger doesn't really support writing to both STDOUT and
  # a file at the same time.  This class implemented the interface the logging
  # class expects for a file handle (mainly a write and a close method) and will
  # optionally write to stdout and a file.  It will also handle file rotation.
  def openFile(logPath)
    @fileIndex += 1
    if @fileIndex > @maxFiles
      @fileIndex = 1
    end

    currentTime = Time.now
    logFileName = "pmt-%d.%d-%d%02d%02d%02d%02d%02d.log" %
      [ Process.pid, @fileIndex, currentTime.year, currentTime.month,
        currentTime.day, currentTime.hour, currentTime.min, currentTime.sec]
    if (logPath != "")
      logFileName = String(logPath) + "/pmt/" + String(logFileName)
    end
    puts "Log file name: %s" % [logFileName]
    
    if @fileHandle != nil
      @fileHandle.close
    end
    @fileHandle = File.open(logFileName, "w")
  end
  
  def initialize(logToStdout = true, logPath = "")
    @logToStdout = logToStdout
    @logPath = logPath
    @fileSize = 0
    @fileIndex = 0
    @fileHandle = nil
    
    @maxFiles = 9
    @maxFileSize = 1048576
    
    self.openFile(@logPath)
  end
  
  def write(*args)
    if @fileHandle != nil
      args.each { |arg|
        if arg.length + @fileSize > @maxFileSize
          @fileSize = 0
          self.openFile(@logPath)
        end
        @fileSize += arg.length
        @fileHandle.write(*args)
        @fileHandle.flush
      }
    end
    
    if @logToStdout
      STDOUT.write(*args)
    end
  end
  
  def close
    if @fileHandle != nil
      @fileHandle.close
    end
  end
end

class MPIHandler
  def executables
    @executables
  end

  def createLogger(logToStdout, logPath)
    @logger = Logger.new(LoggerIO.new(logToStdout, logPath))
    @logger.level = Logger::INFO
    @logger.formatter = proc do |severity, datetime, progname, msg|
      "#{datetime}: #{msg}\n"
    end
  end

  def initialize(logToStdout, logPath, onmonDisplay, port)
    @mpiThread = nil
    @executables = []
    @logPath = logPath
    self.createLogger(logToStdout, logPath)
    @onmonDisplay = onmonDisplay
    @shmKey = 1078394880 + port
  end

  def addExecutable(program, host, options)
    # Add an executable to be managed by PMT.  The executable definitions are
    # kept in a list ordered by MPI rank.  The first process added will receive
    # rank 0.
    @executables << {"program" => program, "options" => options, "host" => host,
      "state" => "idle", "exitcode" => -1}
  end

  def removeExecutable(program, host, options)
    @executables.delete({"program" => program, "options" => options, "host" => host,
                          "state" => "idle", "exitcode" => -1})
    @executables.delete({"program" => program, "options" => options, "host" => host,
                          "state" => "finished", "exitcode" => -1})
  end

  def buildMPICommand(configFileHandle, hostsFileHandle, wrapperScript)
    @executables.each_index { |exeIndex|
      if exeIndex != 0
        configFileHandle.write(" :\n")
      end
      optionsHash = @executables[exeIndex]
      configFileHandle.write("-host %s -np 1 %s %s %s" % [optionsHash["host"], 
                                                          wrapperScript, 
                                                          optionsHash["program"],
                                                          optionsHash["options"]])
      hostsFileHandle.write(optionsHash["host"] + "\n")
    }
        
    mpiEnvironmentSetup = "export MV2_ENABLE_AFFINITY=0; export MV2_IBA_HCA=\"mlx4_0\"; "
    displayString = ""
    if @onmonDisplay != nil
      displayString = "-genv DISPLAY " + @onmonDisplay
    end
    logString = ""
    if @logPath != ""
      logString = "-genv ARTDAQ_LOG_ROOT " + @logPath
    end
    mpiCmd = "mpirun %s %s -genv ARTDAQ_SHM_KEY %d -launcher rsh -configfile %s -f %s" %
      [displayString, logString, @shmKey, configFileHandle.path, hostsFileHandle.path]
    configFileHandle.rewind
    hostsFileHandle.rewind
    return mpiEnvironmentSetup + mpiCmd
  end

  def parseOutput(line)
    # Parse the output of the MPI processes.  The shell wrapper scripts that
    # launch the actual exectuables print the following:
    #   STARTING:hostname:executable:command line options
    #   EXITING:hostname:return code:executable:command line options
    parts = line.chomp.split(":")
    if parts.length == 4 and parts[0] == "STARTING"
      @executables.each { |optionsHash|
        if parts[2] == optionsHash["program"] and parts[3] == optionsHash["options"]
          hostname = Socket.gethostname
          hostParts = hostname.chomp.split(".")
          if parts[1] == optionsHash["host"] or
              (optionsHash["host"] == "localhost" and parts[1] == hostParts[0])
            optionsHash["state"] = "running"
            puts "%s on %s is starting." % [parts[2], parts[1]]
            break
          end
        end
      }
    elsif parts.length == 5 and parts[0] == "EXITING"
      @executables.each { |optionsHash|
        if parts[3] == optionsHash["program"] and parts[4] == optionsHash["options"]
          hostname = Socket.gethostname
          hostParts = hostname.chomp.split(".")
          if parts[1] == optionsHash["host"] or
              (optionsHash["host"] == "localhost" and parts[1] == hostParts[0])
            optionsHash["state"] = "finished"
            optionsHash["exitcode"] = parts[2]
            puts "%s on %s is exiting." % [parts[3], parts[1]]
            break
          end
        end
      }
    end
  end

  def handleIO(stdoutHandle, stderrHandle)
    # Poll stdout and stderr for data.  If a complete line has been received
    # pass it to the parsing code.
    stdBuffers = {}
    while true
      readyFDs = IO.select([stdoutHandle, stderrHandle])
      readyReadFDs = readyFDs[0]
      readyReadFDs.each do |fd|
        if not stdBuffers.keys.include?(fd)
          stdBuffers[fd] = fd.read(1)
        else
          stdBuffers[fd] += fd.read(1)
        end
        
        if stdBuffers[fd].count("\n") != 0
          lines = stdBuffers[fd].split("\n")
          if stdBuffers[fd][-1].chr == "\n"
            stdBuffers[fd] = ""
          else
            stdBuffers[fd] = lines.pop
          end
          
          lines.each { |line|
            @logger.info(line.chomp)
            self.parseOutput(line)
          }
        end
      end
    end
  end

  def start()
    # Use mpirun_rsh to launch all of the configured applications.  This will
    # create temporary config files and pass those to mpirun_rsh.  The output
    # will be monitored for prompts from the wrapper script and will use those
    # to update the state of the application in the executables member data.
    #
    # Note that this will immediately spawn a new thread to handle monitoring
    # MPI and return.
    #
    # First step is to iterate through all of the configured executables and
    # make sure that everything is idle and nothing has been started already.
    # 16-Jul-2013, KAB: also allow starts from the finished state.
    @executables.each { |optionsHash|
      if optionsHash["state"] != "idle" and optionsHash["state"] != "finished"
        return
      end
    }

    mpiThread = Thread.new() do
      configFile = Tempfile.new("config")
      hostsFile = Tempfile.new("hosts")
      begin
        mpiCmd = self.buildMPICommand(configFile, hostsFile, "pmt_mpiwrapper.sh")
        Open3.popen3(mpiCmd) { |stdin, stdout, stderr|
          self.handleIO(stdout, stderr)
        }
      ensure
        configFile.close!
        hostsFile.close!
      end
      
      # If we got here and find applications that are not in the finished state
      # it more than likely means that mpirun_rsh exited and we don't really
      # know what is happening with our MPI application.
      @executables.each { |optionsHash|
        if optionsHash["state"] != "finished"
            optionsHash["state"] = "interrupted"
        end
        }
    end
  end

  def stop
    # 15-Sep-2013, KAB - with mpirun in mvapich2 1.9, it seems like the "right"
    # way to stop the MPI program is to send a signal to mpirun - the previous
    # method of killing off the children no longer reliably kills all of them
    # So, I am commenting out the old way and switching to the new way.
    # Here is the old comment from Steve:
    ## I have yet to find a good way to clean up.  Killing off the mpirun_rsh
    ## process does nothing to the child processes.  For the time being we'll use
    ## the sledge hammer approach and use MPI to submit jobs to kill off everything
    ## that we spawned.
    configFile = Tempfile.new("config")
    hostsFile = Tempfile.new("hosts")
    begin
      Open3.popen3("killall -15 mpirun; sleep 1; killall -9 mpirun") { |stdin, stdout, stderr|
        # Block until this is done.
        stdout.each { |line|
          puts line
        }
      }

      # This isn't the best but after stdout is closed it seems MPI still needs
      # some time to clean everything up.
      sleep(1)
    ensure
      configFile.close!
      hostsFile.close!
    end

    # 16-Jul-2013, KAB: set the status for each executable to finished in
    # case all of the MPI exit messages were not captured and processed
    @executables.each { |optionsHash|
      optionsHash["state"] = "finished"
    }
  end
end

class PMTRPCHandler
  def initialize(mpiHandler)
    @mpiHandler = mpiHandler
  end

  def alive
    # Trivial method used by the unit test to verify that PMT is reachable via
    # XMLRPC.
    return true
  end

  def status
    # Iterate through all of the executables currently configured and generate
    # a list that can be returned to the caller.
    statusItems = []
    @mpiHandler.executables.each { |optionsHash|
      statusItems << optionsHash
    }

    return statusItems
  end

  def addExecutables(configList)
    configList = eval(configList)
    # Add one or more applications to the list of applications that PMT is 
    # managing. Note that this must be done before startSystem is called. 
    configList.each do |configItem|
      @mpiHandler.addExecutable(configItem["program"], configItem["host"],
                                configItem["port"])
    end
  end

  def removeExecutables(configList)
    configList = eval(configList)
    # Add one or more applications to the list of applications that PMT is 
    # managing. Note that this must be done before startSystem is called. 
    configList.each do |configItem|
      @mpiHandler.removeExecutable(configItem["program"], configItem["host"],
                                   configItem["port"])
    end
  end

  def startSystem
    # Have the MPI handler code launch any applications that have been
    # configured.
    @mpiHandler.start
    return true
  end

  def stopSystem
    # Shut down any applications that have been configured.
    @mpiHandler.stop
    return true
  end

  def exit
    # send a TERM signal to ourself to initiate the exit sequence
    Process.kill("TERM", 0)
    return true
  end
end

class PMT
  def initialize(parameterFile, portNumber, logToStdout, logPath, onmonDisplay)
    @rpcThread = nil
    @mpiHandler = MPIHandler.new(logToStdout, logPath, onmonDisplay, portNumber)

    if parameterFile != nil
      IO.foreach(parameterFile) { |definition|
        program, host, port = definition.split(" ")
        @mpiHandler.addExecutable(program, host, port)
      }
    end
    
    # Instantiate the RPC handler and then create the RPC server.
    @rpcHandler = PMTRPCHandler.new(@mpiHandler)
    @rpcServer = XMLRPC::Server.new(port = portNumber)
    @rpcServer.add_handler("pmt", @rpcHandler)
  end

  def start
    # If we've been passed our executable list via the command line we need
    # to spawn off the MPI processes right away.
    if @mpiHandler.executables.size > 0
      @mpiHandler.start
    end

    # Startup the RPC server in a new thread so that we can actually return from
    # here.
    @rpcThread = Thread.new() do
      @rpcServer.serve
    end
  end

  def stop
    # Shutdown the RPC server and prompt the MPI handler class to attempt
    # to clean up.
    @rpcServer.shutdown
    if @rpcThread != nil
      @rpcThread.exit
    end
    @mpiHandler.stop
  end
end

if __FILE__ == $0
  options = OpenStruct.new
  options.logToStdout = true
  options.portNumber = 8080
  options.parameterFile = nil
  options.doCleanup = false
  options.onmonDisplay = nil
  options.logPath = ""

  optParser = OptionParser.new do |opts|
    opts.banner = "Usage: pmt.rb [options]"
    opts.separator ""
    opts.separator "Specific options:"
    opts.on("-p", "--port [PORT]",
            "The port PMT will use for XMLRPC communications.") do |port|
      options.portNumber = Integer(port)
    end
  
    opts.on("-q", "--quiet", "Don't log output to STDOUT.") do |stdout|
      options.logToStdout = false
    end
  
    opts.on("-c", "--cleanup", "Cleanup a set of processes launched by another",
            "instance of PMT.  Note that a definition file must",
            "be specified for this to work.") do |cleanup|
      options.doCleanup = true
    end
    
    opts.on("-d", "--definitions [definition file]",
            "The list of programs and port numbers PMT will manage.") do |defs|
      options.parameterFile = defs
    end

    opts.on("-x", "--display [display]",
            "The X display to use for online monitoring.") do |disp|
      options.onmonDisplay = disp
    end

    opts.on("-l", "--logpath [path]",
            "Root directory for log files.") do |path|
      options.logPath = path
    end

    opts.on_tail("-h", "--help", "Show this message.") do
      puts opts
      exit
    end
  end

  optParser.parse(ARGV)
  if ARGV.length == 0
    puts optParser.help
    exit
  end

  pmt = PMT.new(options.parameterFile, options.portNumber,
                options.logToStdout, options.logPath,
                options.onmonDisplay)

  if options.doCleanup and options.parameterFile == nil
    puts "A program definition file needs to be specified for the cleanup"
    puts "option to work."
    exit
  elsif options.doCleanup
    pmt.stop
  else
    pmt.start
  end

  signals = %w[INT TERM HUP] & Signal.list.keys
  signals.each { 
    |signal| trap(signal) { 
      puts "Cleaning up.  Please wait for PMT to exit..."
      pmt.stop 
      exit
    } 
  }

  while true
    sleep(1)
  end
end
