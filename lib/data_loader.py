from lib.config_loader import ConfigLoader


class DataLoader:

    def __int__(self, env, spark):
        self.env = env
        self.spark = spark
        # self.conf_loader = ConfigLoader()

    @staticmethod
    def get_account_dll():
        schema = """load_date date,active_ind int,account_id string,
            source_sys string,account_start_date timestamp,
            legal_title_1 string,legal_title_2 string,
            tax_id_type string,tax_id string,branch_code string,country string"""
        return schema

    @staticmethod
    def get_party_dll():
        schema = """load_date date,account_id string,party_id string,
        relation_type string,relation_start_date timestamp"""
        return schema

    @staticmethod
    def get_address_dll():
        schema = """load_date date,party_id string,address_line_1 string,
        address_line_2 string,city string,postal_code string,
        country_of_address string,address_start_date date"""
        return schema

    def load_data(self, location, load_filter, schema):
        runtime_filter = ConfigLoader.get_conf_filter(self.env, load_filter)
        return self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .load(location) \
            .where(runtime_filter)

    def load_accounts(self):
        self.load_data('test_data/accounts/',
                       'account.filter', self.get_account_dll())

    def load_parties(self):
        self.load_data('test_data/parties/',
                       'party.filter', self.get_party_dll())

    def load_party_address(self):
        self.load_data('test_data/party_address/',
                       'address.filter', self.get_address_dll())
