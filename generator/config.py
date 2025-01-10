from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_host: str
    redis_port: int
    num_customers: int = 100_000

    class Config:
        env_file = ".env"


settings = Settings()
