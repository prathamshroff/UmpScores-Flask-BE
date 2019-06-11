import json
import boto3
class Dataset:
    """ A python singleton """

    class __impl:
        """ Implementation of the singleton interface """

        def __init__(self):
        	self.iam_role = json.load(open('config.json'))['iam-user']
        	self.refrating = boto3.resource('dynamodb',
        		aws_access_key_id = self.iam_role['key'], 
				aws_secret_access_key = self.iam_role['secret'],
				region_name='us-east-1'
			).Table('refrating')


    __instance = None

    def __init__(self):
        """ Create singleton instance """
        if Dataset.__instance is None:
            Dataset.__instance = Dataset.__impl()

        # Store instance reference as the only member in the handle
        self.__dict__['_Singleton__instance'] = Dataset.__instance

    def __getattr__(self, attr):
        return getattr(self.__instance, attr)

    def __setattr__(self, attr, value):
        return setattr(self.__instance, attr, value)
