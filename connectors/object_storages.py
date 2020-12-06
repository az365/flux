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
    # from connectors import abstract as ac
except ImportError:  # Apparently no higher-level package has been imported, fall back to a local import.
    from .. import context as fc
    from .. import fluxes as fx
    from .. import conns as cs
    from ..connectors import abstract as ac
    from ..utils import (
        arguments as arg,
        log_progress,
    )
    # from ..connectors import abstract as ac


AUTO = arg.DEFAULT
CHUNK_SIZE = 8192
PATH_DELIMITER = '/'


class S3Storage(ac.AbstractConnector):
    def __init__(
            self,
            service_name='s3',
            path_prefix='s3://',
            endpoint_url='https://storage.yandexcloud.net',
            access_key=None,
            secret_key=None,
            context=None,
    ):
        self.service_name = service_name
        self.path_prefix = path_prefix
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.context = context
        self.buckets = dict()

    def bucket(self, name, access_key=AUTO, secret_key=AUTO):
        bucket = self.buckets.get(name)
        if not bucket:
            bucket = S3Bucket(
                path=self.path_prefix + PATH_DELIMITER + name,
                storage=self.storage,
                access_key=arg.undefault(access_key, self.access_key),
                secret_key=arg.undefault(secret_key, self.secret_key),
            )
        return bucket


class S3Bucket(ac.AbstractFolder):
    def __init__(
            self,
            path,
            context=None,
            verbose=True,
            storage=None,
            access_key=None,
            secret_key=None,
    ):
        super().__init__(
            path=path,
            context=context,
            verbose=verbose
        )
        self.storage = storage
        self.access_key = access_key
        self.secret_key = secret_key
        self.session = None
        self.client = None
        self.resource = None

    def get_name(self):
        return self.get_path_as_list()[0]

    def get_delimiter(self):
        return PATH_DELIMITER

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
            props = dict(
                service_name=self.service_name,
                endpoint_url=self.endpoint_url,
            )
        self.client = self.get_session().client(**props)

    def get_resource(self, props=None):
        if not self.resource:
            self.reset_resource(props)
        return self.resource

    def reset_resource(self, props=None):
        if not props:
            props = dict(
                service_name=self.service_name,
                endpoint_url=self.endpoint_url,
            )
        self.resource = self.get_session().resource(**props)


    def get_path_in_bucket(self):
        return self.get_path_as_list()[1:]

    def list_objects(self, params=None, v2=False, field='Contents'):
        if not params:
            params = dict()
        if 'Bucket' not in params:
            params['Bucket'] = self.get_bucket_name()
        if 'Delimiter' not in params:
            params['Delimiter'] = PATH_DELIMITER
        session = self.get_session()
        if v2:
            objects = session.list_objects_v2(**params)
        else:
            objects = session.list_objects(**params)
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

    def get_object(self, object_path):
        return self.get_resource().Object(self.get_bucket_name(), object_path)

    def get_buffer(self, object_path):
        buffer = io.BytesIO()
        self.get_object(object_path).download_fileobj(buffer)
        return buffer
