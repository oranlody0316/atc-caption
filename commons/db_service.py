from abc import ABC, abstractmethod
from typing import Optional
import os

import sqlalchemy
from pyliquibase import Pyliquibase


class BaseDBConnection(ABC):
    user: str
    password: Optional[str]
    host: str
    dbname: str
    
    def __init__(self, user: str, password: Optional[str], host: str, dbname: str):
        self.user = user
        self.password = password
        self.host = host
        self.dbname = dbname
    
    @abstractmethod    
    def close(self):
        pass

class LiquiBaseMySQLConnection(BaseDBConnection):
    
    _property_file_path = "db/liquibase.properties"
    
    def __init__(
                self, 
                user: str, 
                password: str, 
                host: str, 
                dbname: str,
                change_log_file: str,
                log_level: str = "INFO",
        ):
            super().__init__(
                user=user,
                password=password,
                host=host,
                dbname=dbname
            )
            self.change_log_file = change_log_file
            self.log_level = log_level

            
    def establish(self):
        config = f"""
        changeLogFile: {self.changelogfile}
        driver: com.mysql.cj.jdbc.Driver
        url: jdbc:mysql:{self.engine}:@{self.host}:{self.dbname}
        username: {self.user}
        password: {self.password}
        """
        with open(self._property_file_path, mode="w") as f:
            f.write(config)
        self.liquibase = Pyliquibase(
            self._property_file_path,
            logLevel=self.log_level
        )
        return self.liquibase
            
    def close(self):
        if os.path.exists(self._property_file_path):
            os.remove(self._property_file_path)


class SQLAlchemyMySQLConnection(BaseDBConnection):
    def __init__(self, user: str, password: Optional[str], host: str, dbname: str):
        super().__init__(
                user=user,
                password=password,
                host=host,
                dbname=dbname
            )
        self.engine = sqlalchemy.create_engine(
            f'mysql+pymysql://{user}:{password}@{host}/{dbname}'
        )
        
    def establish(self):
        return self.engine
        
    def close(self):
        self.engine.dispose()