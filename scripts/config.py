#!/usr/bin/env python3
"""
Конфигурация для системы резервного копирования Bitrix24
"""

import os
import yaml
import configparser
from typing import List, Optional, Dict


class BackupConfig:
    """Класс для управления конфигурацией backup системы"""
    
    def __init__(self, config_file: str = "config.yaml"):
        """
        Инициализация конфигурации
        
        Args:
            config_file: Путь к файлу конфигурации (YAML). 
        """
        self.config_file = config_file
        
        # Загружаем конфигурацию из YAML файла
        if os.path.exists(config_file):
            self._load_config_file(config_file)
        else:
            raise FileNotFoundError(f"Файл конфигурации не найден: {config_file}")
        
        # Устанавливаем атрибуты для удобного доступа
        self._set_attributes()
    
    def _load_config_file(self, config_file: str):
        """Загрузка конфигурации из YAML файла"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            print(f"Конфигурация загружена из: {config_file}")
            
        except Exception as e:
            raise Exception(f"Ошибка загрузки конфигурации из {config_file}: {e}")
    
    def _set_attributes(self):
        """Устанавливаем атрибуты класса для удобного доступа"""
        for key, value in self.config.items():
            setattr(self, key, value)
    
    def _parse_mysql_config(self, mysql_config_path: str) -> Dict[str, str]:
        """
        Парсинг .my.cnf файла для получения параметров подключения к MySQL
        
        Args:
            mysql_config_path: Путь к .my.cnf файлу
            
        Returns:
            Dict с параметрами подключения: user, password, host, port
        """
        if not os.path.exists(mysql_config_path):
            raise FileNotFoundError(f"MySQL конфигурационный файл не найден: {mysql_config_path}")
        
        config = configparser.ConfigParser()
        config.read(mysql_config_path)
        
        # Читаем секцию [client]
        if 'client' not in config:
            raise ValueError(f"Секция [client] не найдена в {mysql_config_path}")
        
        client_section = config['client']
        
        mysql_params = {
            'user': client_section.get('user', ''),
            'password': client_section.get('password', ''),
            'host': client_section.get('host', ''),
            'port': client_section.get('port', ''),
            'socket': client_section.get('socket', '')
        }
        
        # Проверяем обязательные параметры
        if not mysql_params['user']:
            raise ValueError(f"Параметр 'user' не найден в секции [client] файла {mysql_config_path}")
        if not mysql_params['password']:
            raise ValueError(f"Параметр 'password' не найден в секции [client] файла {mysql_config_path}")
        
        # Проверяем что указан либо socket, либо host
        has_socket = bool(mysql_params['socket'])
        has_host = bool(mysql_params['host'])
        
        if not has_socket and not has_host:
            raise ValueError(f"Должен быть указан либо 'socket', либо 'host' в секции [client] файла {mysql_config_path}")
        
        # Устанавливаем значения по умолчанию для TCP подключения
        if has_host and not mysql_params['port']:
            mysql_params['port'] = '3306'
        
        return mysql_params
    
    def get_mysql_params(self) -> Dict[str, str]:
        """
        Получение параметров подключения к MySQL из .my.cnf
        
        Returns:
            Dict с параметрами: user, password, host, port
        """
        return self._parse_mysql_config(self.MYSQL_CONFIG)
    

    
    def get_smtp_params(self) -> Optional[Dict[str, str]]:
        """
        Получение параметров подключения к SMTP из config.yaml
        
        Returns:
            Dict с параметрами SMTP или None если не настроено
        """
        if not hasattr(self, 'SMTP_CONFIG') or not self.SMTP_CONFIG or not isinstance(self.SMTP_CONFIG, dict):
            return None
        
        smtp_params = {
            'server': self.SMTP_CONFIG.get('server', ''),
            'port': self.SMTP_CONFIG.get('port', 587),
            'username': self.SMTP_CONFIG.get('username', ''),
            'password': self.SMTP_CONFIG.get('password', ''),
            'use_tls': self.SMTP_CONFIG.get('use_tls', True)
        }
        
        # Проверяем обязательные параметры
        if not smtp_params['server'] or not smtp_params['username'] or not smtp_params['password']:
            return None
            
        return smtp_params
    

    
    def get_s3_params(self) -> Optional[Dict[str, str]]:
        """
        Получение параметров подключения к S3 из config.yaml
        
        Returns:
            Dict с параметрами S3 или None если не настроено
        """
        if not hasattr(self, 'S3_CONFIG') or not self.S3_CONFIG or not isinstance(self.S3_CONFIG, dict):
            return None
        
        s3_params = {
            'endpoint_url': self.S3_CONFIG.get('endpoint_url', ''),
            'bucket_name': self.S3_CONFIG.get('bucket_name', ''),
            'access_key': self.S3_CONFIG.get('access_key', ''),
            'secret_key': self.S3_CONFIG.get('secret_key', ''),
            'backup_path': self.S3_CONFIG.get('backup_path', 'backups'),
            'max_backups': self.S3_CONFIG.get('max_backups', 5),
            'delete_local_after_upload': self.S3_CONFIG.get('delete_local_after_upload', False)
        }
        
        # Проверяем обязательные параметры
        required_params = ['endpoint_url', 'bucket_name', 'access_key', 'secret_key']
        for param in required_params:
            if not s3_params[param]:
                return None
        
        return s3_params
    
    def get_s3_work_storage_params(self) -> Optional[Dict[str, str]]:
        """
        Получение параметров подключения к S3 файловому хранилищу Bitrix из config.yaml
        
        Returns:
            Dict с параметрами S3 рабочего хранилища или None если не настроено
        """
        if not hasattr(self, 'S3_WORK_STORAGE_CONFIG') or not self.S3_WORK_STORAGE_CONFIG or not isinstance(self.S3_WORK_STORAGE_CONFIG, dict):
            return None
        
        s3_work_params = {
            'endpoint_url': self.S3_WORK_STORAGE_CONFIG.get('endpoint_url', ''),
            'bucket_name': self.S3_WORK_STORAGE_CONFIG.get('bucket_name', ''),
            'access_key': self.S3_WORK_STORAGE_CONFIG.get('access_key', ''),
            'secret_key': self.S3_WORK_STORAGE_CONFIG.get('secret_key', ''),
            'backup_folder': self.S3_WORK_STORAGE_CONFIG.get('backup_folder', 's3-work-file-storage'),
            'max_backups': self.S3_WORK_STORAGE_CONFIG.get('max_backups', 5)
        }
        
        # Проверяем обязательные параметры
        required_params = ['endpoint_url', 'bucket_name', 'access_key', 'secret_key']
        for param in required_params:
            if not s3_work_params[param]:
                return None
        
        return s3_work_params