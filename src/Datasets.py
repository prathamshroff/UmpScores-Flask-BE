import pandas as pd
import os
import boto3

class Table():
    """
    The Dataset class is our wrapper around boto3's dynamodb object. We will
    use it to add more robust and safe queries in the future. 

    Parameters
    ----------
    iam_role : dict
        A dictionary containing the iam role for the corresponding 
        dynamodb table. Should look as follows:
            e.g. {
                "key": iam_access_key,
                "secret": iam_secret_key 
            }
    table : str
        A string representing the dynamodb table to use given the iam_role.
            e.g. "Players"

    Attributes
    ----------
    iam_role : dict
        This is where we store iam_role
    dynamodb : boto3.resource object
        Boto3 resoure object for dynamodb. Uses iam_role and the table parameter
        to connect to dynamodb from aws. We use this object for all interactions
        with dynamodb.
    """
    def __init__(self, iam_role, table, cloudsearchpp):
        self.iam_role = iam_role
        self.dynamodb = boto3.resource('dynamodb',
            aws_access_key_id=self.iam_role['key'],
            aws_secret_access_key=self.iam_role['secret'],
            region_name='us-east-1'
            ).Table(table)
        self.cloudsearchpp = cloudsearchpp

    def __fillna(df, string_fields):
        """Fills in empty fields with -1 for number values and 'n/a' for strings
        """
        values = {key: 'n/a' for key in string_fields}

        number_fields = list(df.columns)
        for column in string_fields:
            number_fields.remove(column)

        values.update({key: -1 for key in number_fields})

        df = df.fillna(value=values)
        return df

    def make_umps():
        """Makes refined/2019.csv
        """
        string_fields = [
            'Data source',
            'ump',
            'name'
        ]
        profiles = pd.read_excel('datasets/raw/umpire2019.xlsx')
        stats = pd.read_csv('datasets/raw/umpire_bcr_2019.csv')
        df = pd.merge(profiles, stats, left_on='ump', right_on='name')
        dataset = __fillna(df, string_fields)
        dataset = dataset.drop(columns=['Unnamed: 0', 'name']) 
        dataset.to_csv('datasets/refined/umps2019.csv', index=False)
        print(dataset.columns)

    def get(self, query_map, filter_expressions=None):
        """
        Get method takes some query map and some filter options
        then returns a json object representing that entire item entry within dynamodb

        Parameters
        ----------
        queryMap : dict
                Query the dynamodb table for the following key value pairs. Make
                sure you give the primary/partition key as a key value pair within
                this dictionary.
                        e.g. {"Umpires": "jordan baker", ...}
        FilterExpressions : boto3.dynamodb.conditions.(Attr/Key)
                Filter the results given some attribute/key filter option. If None,
                FilterExpressions will not be sent.
                        e.g. Attr('timeStamp').between(start, end)

        Returns
        ----------
        dict
                Matched item element within the database. If no such element exists returns
                empty dict. Response represents an entire element within the table. For
                example, if this dataset is connected to the Umpires Table, the return
                would be a json response of all the statistics about a singular umpire.
                        e.g. {
                                "attr1": "val1",
                                "attr2": "val2",
                                ...
                        }

        """
        if not filter_expressions:
            data = self.dynamodb.get_item(Key=query_map)
        else:
            data = self.dynamodb.get_item(
                Key=query_map, FilterExpressions=filter_expressions)
        if 'Item' not in data:
            return {}
        return data['Item']

    def scan(self, query=None, filter_expressions=None):
        """
        Scan method takes some query keyword and some filter options
        then returns a json response consisting of relevant dynamodb
        elements. 

        Parameters
        ----------
        query : str
                Query the dynamodb table for this following string.
                        e.g. "jordan baker"
        FilterExpressions : boto3.dynamodb.conditions.(Attr/Key)
                Filter the results given some attribute/key filter option. If None,
                FilterExpressions will not be sent.
                        e.g. Attr('timeStamp').between(start, end)

        Returns
        ----------
        list of dictionaries
                Matched item elements within the database. If no such elements exists returns
                empty dict. One dictionary represents an entire item within dynamodb.
                For example, if this dataset is connected to the Umpires table, one dictionary
                would give all the statistics about a single Umpire.
                        e.g. [{
                                "attr1": "val1",
                                "attr2": "val2",
                                ...
                        }, ...]

        """
        if not filter_expressions and not query:
            data = self.dynamodb.scan()
        elif not query and filter_expressions:
            data = self.dynamodb.scan(FilterExpressions=filter_expressions)
        else:
            data = self.dynamodb.scan(
                query=query, FilterExpressions=filter_expressions)
        if 'Items' not in data:
            return {}
        return data['Items']

    #TODO Change this method to take in a dict/pandas.dataframe, gets rid of refined_filepath
    def uploadUmpires(self, refined_filepath): 
        """Uploads every item within some filepath to the dynamodb table
        """
        df = pd.read_csv(refined_filepath, keep_default_na=False)
        data = df.to_dict()
        keys = list(data.keys())
        
        with self.table.batch_writer() as batch:
            # data['number'] = [5, 4, 3, 78, ...] which is an array of values for every row
            for item_id in range(len(data[keys[0]])):
                item = {key: data[key][item_id] for key in keys}
                for key in keys:
                    if type(item[key]) == float or type(item[key]) == int:
                        item[key] = Decimal(str(item[key]))
                try:
                    # cloudsearch cache just means new items were added to dynamodb
                    # therefore we need to add them to cloudsearch
                    self.cloudsearchpp.cache.append(item)
                    batch.put_item(
                        Item=item
                    )
                except botocore.exceptions.ClientError as e:
                    print("Error couldn't upload the following row: \n", item)
                    exit(0)
                except botocore.exceptions.ParamValidationError as e:
                    print("Wrong Item type: {0}".format(item))
                    exit(0)

        print('Dynamodb table for {0} refreshed'.format(self.__table_name))

    def clearTable(self, primary_key, sort_key = None):
        """Deletes every item from this dynamodb table
        """
        scan = self.table.scan()
        with self.table.batch_writer() as batch:
            if sort_key == None:
                for each in scan['Items']:
                    batch.delete_item(
                        Key = {
                            primary_key: each[primary_key]
                    })
            else:
                for each in scan['Items']:
                    batch.delete_item(
                        Key = {
                            primary_key: each[primary_key],
                            sort_key: each[sort_key]
                    })
        print('Dynamodb table for {0} emtpied'.format(self.__table_name))
