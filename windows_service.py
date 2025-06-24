"""
Windows Service wrapper for SQL-Kafka Producer
"""

import sys
import os
import time
import logging
from pathlib import Path

try:
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager
except ImportError:
    print("ERROR: pywin32 is required. Install with: pip install pywin32")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("ERROR: python-dotenv is required. Install with: pip install python-dotenv")
    sys.exit(1)

# Add the project root to Python path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.core.app import SQLKafkaProducer
from src.models.config import AppConfig
import yaml


class SQLKafkaProducerService(win32serviceutil.ServiceFramework):
    """Windows Service for SQL-Kafka Producer"""
    
    _svc_name_ = "SQLKafkaProducer"
    _svc_display_name_ = "SQL to Kafka Data Producer"
    _svc_description_ = "Production service for extracting SQL data and publishing to Kafka topics"
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.is_running = True
        self.app = None
        
        # Setup logging for service
        self.setup_service_logging()
    
    def setup_service_logging(self):
        """Setup logging for Windows service"""
        log_dir = Path("C:/Kafka/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            filename=log_dir / "service.log",
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            filemode='a'
        )
        
        self.logger = logging.getLogger('SQLKafkaProducerService')
    
    def load_environment(self):
        """Load environment variables from .env file"""
        try:
            env_path = "C:/Kafka/config/.env"
            if os.path.exists(env_path):
                self.logger.info(f"Loading environment from: {env_path}")
                load_dotenv(env_path, override=True)
            else:
                self.logger.warning(f".env file not found at: {env_path}")
        except Exception as e:
            self.logger.error(f"Failed to load environment variables: {e}")
    
    def SvcStop(self):
        """Stop the service"""
        self.logger.info("Service stop requested")
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        
        self.is_running = False
        if self.app:
            try:
                self.app.stop()
                self.logger.info("Application stopped successfully")
            except Exception as e:
                self.logger.error(f"Error stopping application: {e}")
        
        win32event.SetEvent(self.hWaitStop)
        self.logger.info("Service stopped")
    
    def SvcDoRun(self):
        """Run the service"""
        self.logger.info("Service starting...")
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        
        try:
            self.main()
        except Exception as e:
            self.logger.error(f"Service error: {e}", exc_info=True)
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_ERROR_TYPE,
                servicemanager.PYS_SERVICE_STOPPED,
                (self._svc_name_, str(e))
            )
    
    def main(self):
        """Main service logic"""
        try:
            # Load environment variables from .env file first
            self.load_environment()
            
            # Load configuration
            config_path = "C:/Kafka/config/config.yaml"
            
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            
            config = AppConfig.from_yaml(config_data)
            
            # Initialize and run application
            self.app = SQLKafkaProducer(config)
            self.app.initialize()
            
            self.logger.info("SQL-Kafka Producer service started successfully")
            
            # Run the application
            self.app.run()
            
        except Exception as e:
            self.logger.error(f"Failed to start application: {e}", exc_info=True)
            raise


def install_service():
    """Install the service"""
    print("Installing SQL-Kafka Producer Windows Service...")
    
    try:
        win32serviceutil.InstallService(
            SQLKafkaProducerService._svc_reg_class_,
            SQLKafkaProducerService._svc_name_,
            SQLKafkaProducerService._svc_display_name_,
            description=SQLKafkaProducerService._svc_description_,
            startType=win32service.SERVICE_AUTO_START,
            bRunInteractive=False
        )
        print("Service installed successfully")
        print(f"Service Name: {SQLKafkaProducerService._svc_name_}")
        print(f"To start: net start {SQLKafkaProducerService._svc_name_}")
    except Exception as e:
        print(f"Failed to install service: {e}")
        return False
    
    return True


if __name__ == '__main__':
    if len(sys.argv) == 1:
        # No arguments - try to start as service
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(SQLKafkaProducerService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        # Handle command line arguments
        if 'install' in sys.argv:
            install_service()
        else:
            win32serviceutil.HandleCommandLine(SQLKafkaProducerService)