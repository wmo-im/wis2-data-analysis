# wis2-data-analysis
#A brief overview of the main components in the script:

ConfigReader Class:
  *Reads configuration files using the configparser module.
  *Used to read various configuration files like keys.ini, general_config.ini, mqtt_config.ini, and database_config.ini.

BufrFileManager Class:
  *Manages the decoding of BUFR files.
  *Downloads files from a given URL and decodes them using the eccodes library.
  *Inserts the decoded BUFR data into a PostgreSQL database using the DatabaseManager class.

DatabaseManager Class:
  *Manages database connections and provides methods for inserting both message data and BUFR data into a PostgreSQL database.
  *Utilizes a connection pool for efficient database connections.

MqttHandler Class:
  *Handles MQTT connections and message handling.
  *Connects to an MQTT broker, subscribes to a specific topic, and processes incoming messages.
  *Validates messages, extracts relevant information, and inserts the data into the PostgreSQL database using the DatabaseManager class.

Main Function (main()):
  *Configures logging and reads various configuration files.
  *Initializes instances of the DatabaseManager and MqttHandler classes, connecting to the MQTT broker and database.

if name == "main":
  *Calls the main() function when the script is executed.
