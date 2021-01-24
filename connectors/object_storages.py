from abc import abstractmethod
import io
import boto3

try:  # Assume we're a sub-module in a package.
    import context as fc
    import fluxes as fx
    import conns as cs
    from connectors import abstract as ac
    from utils import (
        arguments as arg,
        log_progress,
    )
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import context as fc
    from .. import fluxes as fx
    from .. import conns as cs
    from ..connectors import abstract as ac
    from ..utils import (
        arguments as arg,
        log_progress,
    )


AUTO = arg.DEFAULT
CHUNK_SIZE = 8192
DEFAULT_PATH_DELIMITER = '/'
FIRST_PATH_DELIMITER = '://'
DEFAULT_S3_ENDPOINT_URL = 'https://storage.yandexcloud.net'


class AbstractObjectStorage(ac.AbstractStorage):
    def __init__(
            self,
            name,
            context,
            verbose=True,
    ):
        super().__init__(
            name=name,
            context=context,
            verbose=verbose,
        )

    @abstractmethod
    def get_default_child_class(self):
        pass

    @abstractmethod
    def get_service_name(self):
        pass

    def get_path_prefix(self):
        return self.get_service_name() + FIRST_PATH_DELIMITER

    def get_path_delimiter(self):
        return DEFAULT_PATH_DELIMITER


class S3Storage(ac.AbstractObjectStorage):
    def __init__(
            self,
            name='s3',
            endpoint_url=DEFAULT_S3_ENDPOINT_URL,
            access_key=None,
            secret_key=None,
            context=None,
            verbose=True,
    ):
        super().__init__(
            name=name,
            context=context,
            verbose=verbose,
        )
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key

    @staticmethod
    def get_service_name():
        return 's3'

    def get_default_path_delimiter(self):
        return

    @staticmethod
    def get_default_child_class():
        return S3Bucket

    def get_buckets(self):
        return self.children

    def bucket(self, name, access_key=AUTO, secret_key=AUTO):
        bucket = self.get_buckets().get(name)
        if not bucket:
            bucket = S3Bucket(
                name=name,
                storage=self,
                access_key=arg.undefault(access_key, self.access_key),
                secret_key=arg.undefault(secret_key, self.secret_key),
            )
        return bucket

    def get_resource_properties(self):
        return dict(
            service_name=self.get_service_name(),
            endpoint_url=self.endpoint_url,
        )


class S3Bucket(ac.FlatFolder):
    def __init__(
            self,
            name,
            storage,
            verbose=True,
            access_key=AUTO,
            secret_key=AUTO,
    ):
        super().__init__(
            name=name,
            parent=storage,
            verbose=verbose,
        )
        self.storage = storage
        self.access_key = arg.undefault(access_key, self.get_storage().access_key)
        self.secret_key = arg.undefault(secret_key, self.get_storage().secret_key)
        self.session = None
        self.client = None
        self.resource = None

    def get_default_child_class(self):
        return S3Folder

    def folder(self, name, **kwargs):
        return self.child(name, bucket=self, **kwargs)

    def get_bucket_name(self):
        return self.name

    def get_session(self, props=None):
        if not self.session:
            self.reset_session(props)
        return self.session

    def reset_session(self, props=None):
        if not props:
            props = dict(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
        self.session = boto3.session.Session(**props)

    def get_client(self, props=None):
        if not self.client:
            self.reset_client(props)
        return self.client

    def reset_client(self, props=None):
        if not props:
            props = self.get_storage().get_resource_properties()
        self.client = self.get_session().client(**props)

    def get_resource(self, props=None):
        if not self.resource:
            self.reset_resource(props)
        return self.resource

    def reset_resource(self, props=None):
        if not props:
            props = self.get_storage().get_resource_properties()
        self.resource = self.get_session().resource(**props)

    def list_objects(self, params=None, v2=False, field='Contents'):
        if not params:
            params = dict()
        if 'Bucket' not in params:
            params['Bucket'] = self.get_name()
        if 'Delimiter' not in params:
            params['Delimiter'] = self.get_path_delimiter()
        client = self.get_client(self.get_storage().get_resource_properties())
        if v2:
            objects = client.list_objects_v2(**params)
        else:
            objects = client.list_objects(**params)
        if field:
            return objects[field]
        else:
            return objects

    def yield_objects(self, params):
        continuation_token = None
        if 'MaxKeys' not in params:
            params['MaxKeys'] = 1000
        while True:
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            response = self.list_objects(params=params, v2=True, field=None)
            yield from response.get('Contents', [])
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')

    def list_prefixes(self):
        return self.list_objects('CommonPrefixes')

    def get_object(self, object_path_in_bucket):
        return self.get_resource().Object(self.get_bucket_name(), object_path_in_bucket)

    def get_buffer(self, object_path_in_bucket):
        buffer = io.BytesIO()
        self.get_object(object_path_in_bucket).download_fileobj(buffer)
        return buffer


class S3Folder(ac.FlatFolder):
    def __init__(
            self,
            name,
            bucket,
            verbose=AUTO,
    ):
        super().__init__(
            name=name,
            parent=bucket,
            verbose=verbose,
        )

    def get_default_child_class(self):
        return S3Object

    def get_bucket(self):
        return self.parent

    def object(self, name):
        return self.child(name, folder=self)

    def get_buffer(self, object_path_in_bucket):
        return self.get_bucket().get_buffer(object_path_in_bucket)


class S3Object(ac.LeafConnector):
    def __init__(
            self,
            name,
            folder,
            verbose=AUTO,
    ):
        assert isinstance(folder, S3Folder)
        super().__init__(
            name=name,
            parent=folder,
        )
        self.verbose = arg.undefault(verbose, folder.verbose)

    def get_folder(self):
        return self.parent

    def get_bucket(self):
        return self.get_folder().get_bucket()

    def get_object_path_in_bucket(self):
        if self.get_folder().get_name():
            return self.get_folder().get_name() + self.get_path_delimiter() + self.get_name()
        else:
            return self.get_name()

    def get_buffer(self):
        return self.get_folder().get_buffer(self.get_object_path_in_bucket())

    def get_object_response(self):
        return self.get_bucket().get_client().get_object(
            Bucket=self.get_bucket().get_name(),
            Key=self.get_object_path_in_bucket(),
        )

    def get_body(self):
        return self.get_object_response()['Body']

    def get_data(self):
        for line in self.get_body():
            yield line.decode('utf8', errors='ignore')
