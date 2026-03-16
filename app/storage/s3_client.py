import aiobotocore.session


class S3Client:
    def __init__(self, endpoint_url: str, access_key: str, secret_key: str, region: str):
        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region
        self._session = aiobotocore.session.get_session()

    def _client(self):
        return self._session.create_client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region,
        )

    async def upload_object(
        self, bucket: str, key: str, body: bytes, content_type: str
    ) -> str | None:
        async with self._client() as s3:
            response = await s3.put_object(
                Bucket=bucket, Key=key, Body=body, ContentType=content_type
            )
            return response.get("VersionId")

    async def generate_presigned_url(
        self,
        bucket: str,
        key: str,
        expires_in: int = 300,
        disposition: str = "inline",
    ) -> str:
        async with self._client() as s3:
            return await s3.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": bucket,
                    "Key": key,
                    "ResponseContentDisposition": disposition,
                },
                ExpiresIn=expires_in,
            )

    async def copy_object(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str):
        async with self._client() as s3:
            await s3.copy_object(
                Bucket=dst_bucket,
                Key=dst_key,
                CopySource={"Bucket": src_bucket, "Key": src_key},
            )

    async def delete_object(self, bucket: str, key: str):
        async with self._client() as s3:
            await s3.delete_object(Bucket=bucket, Key=key)

    async def get_object(self, bucket: str, key: str) -> bytes:
        async with self._client() as s3:
            resp = await s3.get_object(Bucket=bucket, Key=key)
            return await resp["Body"].read()

    async def head_object(self, bucket: str, key: str) -> dict:
        async with self._client() as s3:
            return await s3.head_object(Bucket=bucket, Key=key)

    async def head_bucket(self, bucket: str):
        async with self._client() as s3:
            await s3.head_bucket(Bucket=bucket)
