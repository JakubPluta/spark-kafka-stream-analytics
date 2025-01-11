from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_host: str = "localhost"
    redis_port: int = 6379
    num_customers: int = 100_000

    class Config:
        env_file = ".env"


settings = Settings()
