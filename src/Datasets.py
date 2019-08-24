import pandas as pd
import os
import boto3
from decimal import Decimal
import botocore
import time
#TODO LIST: 
# Modify scan to iterate through entire list
# TODO limit item to primary and sort keys


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
    RETRY_EXCEPTIONS = ('ProvisionedThroughputExceededException', 
        'ThrottlingException')
    def __init__(self, iam_role, table, cloudsearch=None):
        self.iam_role = iam_role
        self.__table_name = table
        self.dynamodb = boto3.resource('dynamodb',
            aws_access_key_id=self.iam_role['key'],
            aws_secret_access_key=self.iam_role['secret'],
            region_name='us-east-1'
            ).Table(table)
        self.client = boto3.client('dynamodb',
            aws_access_key_id = self.iam_role['key'],
            aws_secret_access_key = self.iam_role['secret'],
            region_name='us-east-1')
        self.cloudsearch = cloudsearch

    @staticmethod
    def fillna(df, string_fields):
        """
        Fills in empty fields with -1 for number values and 'n/a' for strings
        """
        values = {key: 'n/a' for key in string_fields}

        number_fields = list(df.columns)
        for column in string_fields:
            if column in number_fields:
                number_fields.remove(column)

        values.update({key: -1 for key in number_fields})

        df = df.fillna(value=values)
        return df
        
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

    def scan(self, **kwargs):
        """
        Scan method takes some query keyword and some filter options
        then returns a json response consisting of relevant dynamodb
        elements. 

        Parameters
        ----------
        query : str
                Query the dynamodb table for this following string.
                        e.g. "jordan baker"
        filter_expressions : boto3.dynamodb.conditions.(Attr/Key)
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
        data = self.dynamodb.scan(**kwargs)
        # if not filter_expressions:
        #     data = self.dynamodb.scan()
        # else:
        #     data = self.dynamodb.scan(FilterExpressions=filter_expressions)
        if 'Items' not in data:
            return {}
        return data['Items']

    #TODO Change this method to take in a dict/pandas.dataframe, gets rid of refined_filepath
    def uploadFilepath(self, refined_filepath, backoff_init = 50): 
        """Uploads every item within some filepath to the dynamodb table
        """
        df = pd.read_csv(refined_filepath, keep_default_na=False)
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
        data = df.to_dict()
        keys = list(data.keys())
        
        # data['number'] = [5, 4, 3, 78, ...] which is an array of values for every row
        for item_id in range(len(data[keys[0]])):
            item = {key: data[key][item_id] for key in keys}
            for key in keys:
                if type(item[key]) == float or type(item[key]) == int:
                    item[key] = Decimal(str(item[key]))
            backoff = backoff_init
            while True:
                try:
                    # cloudsearch cache just means new items were added to dynamodb
                    # therefore we need to add them to cloudsearch
                    self.dynamodb.put_item(
                        Item=item
                    )
                    if self.cloudsearch != None:
                        self.cloudsearch.cache.append(item)
                    time.sleep(backoff / 1000)
                    break
                except botocore.exceptions.ParamValidationError as e:

                    print("Wrong Item type")
                    print(e)
                    exit(0)
                except botocore.exceptions.ClientError as e:
                    errcode = e.response['Error']['Code']
                    if errcode in Table.RETRY_EXCEPTIONS:
                        backoff *= 2
                        if (backoff / 1000 > 60):
                            print('Exponential Backoff Failed. Load too heavy!')
                            exit(1)
                        else:
                            print('Increasing backoff to {0}'.format(backoff))
                    else:
                        print(e)
                        exit(1)
                except Exception as e:
                    print("Unknown Error {0}".format(e))
                    return


        print('Dynamodb table for {0} refreshed'.format(self.__table_name))

    def clearTable(self, primary_key, sort_key = None, backoff_init = 50):
        """Deletes every item from this dynamodb table
        """
        
        paginator = self.client.get_paginator('scan')
        operation_parameters = {
            'TableName': self.__table_name
        }
        page_iterator = paginator.paginate(**operation_parameters)
        for scan in page_iterator:
            backoff = backoff_init
            while True:
                try:
                    with self.dynamodb.batch_writer() as batch:
                        for each in scan['Items']:
                            primary_type = 'N' if 'N' in each[primary_key] else 'S'
                            if sort_key == None:
                                key = {
                                    'Key': {
                                        primary_key: each[primary_key][primary_type]
                                    }
                                }
                            else:
                                sort_type = 'N' if 'N' in each[sort_key] else 'S'
                                key = {
                                    'Key': {
                                        primary_key: each[primary_key][primary_type],
                                        sort_key: Decimal(each[sort_key][sort_type])
                                    }
                                }
                                batch.delete_item(**key)
                                time.sleep(backoff / 1000)
                                print('Deleted pk: {0}, sk: {1}'.format(each[primary_key][primary_type], 
                                    each[sort_key][sort_type]))
                    break
                except botocore.exceptions.ClientError as e:
                    errcode = e.response['Error']['Code']
                    if errcode in Table.RETRY_EXCEPTIONS:
                        backoff *= 2
                        if (backoff / 1000 > 60):
                            print('Exponential Backoff Failed. Load too heavy!')
                            exit(0)
                        else:
                            print('Increasing backoff to {0}'.format(backoff))
                    elif errcode == 'ValidationException':
                        print('Incorrect primary or sort key, used')
                        return
                    else:
                        print(e)
                except Exception as e:
                    print('Uncaught super exception: {0}'.format(e))


        print('Dynamodb table for {0} emtpied'.format(self.__table_name))
