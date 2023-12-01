# config_reader.py
import configparser

class ConfigReader:
    @staticmethod
    def read_config(file_path, section):
        try:
            config = configparser.ConfigParser()
            config.read(file_path)
            return config[section]
        except (configparser.Error, FileNotFoundError) as e:
            # Log the error or handle it according to your needs
            print(f"Error reading config file '{file_path}': {e}")
            return {}
