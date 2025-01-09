from pydantic import BaseSettings


class Settings(BaseSettings):
    redis_host: str = "localhost"
    redis_port: int = 6379

    class Config:
        env_file = ".env"
