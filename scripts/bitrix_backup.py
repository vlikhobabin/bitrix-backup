#!/usr/bin/python3
"""
Bitrix24 Backup System (Python Version)
Создает полный backup системы с ротацией файлов
"""

import os
import sys
import shutil
import subprocess
import tarfile
import logging
import logging.handlers
import fnmatch
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import tempfile
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# S3 для облачного хранения
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    S3_AVAILABLE = True
except ImportError:
    # Определяем заглушки для исключений если boto3 недоступен
    class ClientError(Exception):
        pass
    class NoCredentialsError(Exception):
        pass
    S3_AVAILABLE = False

from config import BackupConfig


class BitrixBackup:
    """Основной класс для создания резервных копий Bitrix24"""
    
    def __init__(self):
        """Инициализация с загрузкой конфигурации"""
        self.config = BackupConfig()
        self.temp_dir = None
        self.logger = self._setup_logging()
        self.included_files = []
        self.excluded_files = []
        
    def _setup_logging(self) -> logging.Logger:
        """Настройка логирования в файл с автоматической ротацией"""
        logger = logging.getLogger('bitrix-backup')
        
        # Устанавливаем уровень логирования из конфигурации
        log_level = getattr(logging, self.config.LOG_LEVEL.upper(), logging.INFO)
        logger.setLevel(log_level)
        
        # Убираем существующие handlers чтобы избежать дублирования
        logger.handlers.clear()
        
        # Создаем директорию для логов если её нет
        os.makedirs(self.config.LOG_DIR, exist_ok=True)
        
        # Файловый handler с ротацией по размеру
        log_file = os.path.join(self.config.LOG_DIR, 'bitrix_backup.log')
        max_bytes = self.config.LOG_MAX_SIZE_MB * 1024 * 1024  # Конвертируем MB в байты
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, 
            maxBytes=max_bytes, 
            backupCount=self.config.LOG_BACKUP_COUNT,
            encoding='utf-8'
        )
        file_formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(message)s', 
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Console handler для вывода на экран
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter('[%(asctime)s] %(message)s', 
                                            datefmt='%Y-%m-%d %H:%M:%S')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    def _should_exclude(self, file_path: str, pattern: str) -> bool:
        """
        Проверяет, должен ли файл быть исключен на основе паттерна
        
        Args:
            file_path: Относительный путь к файлу
            pattern: Паттерн исключения
            
        Returns:
            True если файл нужно исключить
        """
        # Нормализуем пути
        file_path = file_path.replace('\\', '/')
        pattern = pattern.replace('\\', '/')
        
        # 1. Проверяем wildcard паттерны для имен файлов (*.log, *.tmp)
        if '*' in pattern or '?' in pattern:
            # Если паттерн содержит /, проверяем полный путь
            if '/' in pattern:
                return fnmatch.fnmatch(file_path, pattern)
            else:
                # Иначе проверяем только имя файла
                filename = os.path.basename(file_path)
                return fnmatch.fnmatch(filename, pattern)
        
        # 2. Проверяем директории (bitrix/cache/, local/temp/)
        if '/' in pattern:
            # Для директорий проверяем точное совпадение начала пути
            pattern_normalized = pattern.rstrip('/') + '/'
            return file_path.startswith(pattern_normalized)
        
        # 3. Проверяем точные имена файлов (.DS_Store, Thumbs.db)
        filename = os.path.basename(file_path)
        if filename == pattern:
            return True
        
        # 4. Проверяем имя файла в любой директории (для паттернов без /)
        if not '/' in pattern and not '*' in pattern and not '?' in pattern:
            return filename == pattern
        
        return False
    
    def log_message(self, message: str):
        """Логирование сообщения"""
        self.logger.info(message)
    
    def log_error(self, message: str):
        """Логирование ошибки"""
        self.logger.error(f"ERROR: {message}")
    
    def check_disk_space(self) -> bool:
        """Проверка доступного места на диске"""
        try:
            stat = shutil.disk_usage(self.config.BACKUP_DIR)
            available_kb = stat.free // 1024
            required_kb = self.config.MIN_DISK_SPACE_KB
            
            if available_kb < required_kb:
                self.log_error(f"Недостаточно места на диске. "
                             f"Доступно: {available_kb}KB, Требуется: {required_kb}KB")
                return False
            
            self.log_message(f"Проверка места на диске: OK ({available_kb}KB доступно)")
            return True
        except Exception as e:
            self.log_error(f"Ошибка проверки места на диске: {e}")
            return False
    
    def cleanup_temp(self):
        """Очистка временной директории"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
            except Exception as e:
                self.log_error(f"Ошибка очистки временной директории: {e}")
    
    def backup_database(self) -> bool:
        """Создание backup базы данных"""
        self.log_message("Начинаем backup базы данных...")
        
        try:
            db_backup_file = os.path.join(self.temp_dir, f"database_{self.config.DB_NAME}.sql")
            
            # Используем --defaults-file для безопасного подключения без передачи пароля в командной строке
            cmd = [
                'mysqldump',
                f'--defaults-file={self.config.MYSQL_CONFIG}',
                '--single-transaction',
                '--routines',
                '--triggers',
                '--lock-tables=false',
                self.config.DB_NAME
            ]
            
            with open(db_backup_file, 'w') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
            
            if result.returncode == 0:
                size = self._get_human_size(db_backup_file)
                self.log_message(f"Backup базы данных создан: {size}")
                return True
            else:
                self.log_error(f"Ошибка mysqldump: {result.stderr}")
                return False
                
        except Exception as e:
            self.log_error(f"Ошибка создания backup'а базы данных: {e}")
            return False
    
    def backup_files(self) -> bool:
        """Создание backup файлов Bitrix"""
        self.log_message("Начинаем backup файлов Bitrix...")
        
        try:
            files_backup = os.path.join(self.temp_dir, "bitrix_files.tar.gz")
            
            # Очищаем списки для текущего backup'а
            self.included_files.clear()
            self.excluded_files.clear()
            
            # Создаем tar архив с исключениями
            with tarfile.open(files_backup, "w:gz") as tar:
                excluded_count = 0
                included_count = 0
                
                def filter_func(tarinfo):
                    nonlocal excluded_count, included_count
                    
                    # Получаем относительный путь от корня Bitrix
                    rel_path = os.path.relpath(tarinfo.name, os.path.basename(self.config.BITRIX_ROOT))
                    if rel_path.startswith('..'):
                        rel_path = tarinfo.name
                    
                    # Проверяем каждый паттерн исключения
                    excluded_by_pattern = None
                    for pattern in self.config.EXCLUDE_PATTERNS:
                        # Проверяем разные типы паттернов (НЕ убираем слеши!)
                        if self._should_exclude(rel_path, pattern):
                            excluded_count += 1
                            # Сохраняем информацию об исключенном файле
                            self.excluded_files.append({
                                'path': rel_path,
                                'size': tarinfo.size if tarinfo.isfile() else 0,
                                'type': 'file' if tarinfo.isfile() else 'directory',
                                'excluded_by_pattern': pattern
                            })
                            return None
                    
                    # Файл включается в backup
                    included_count += 1
                    self.included_files.append({
                        'path': rel_path,
                        'size': tarinfo.size if tarinfo.isfile() else 0,
                        'type': 'file' if tarinfo.isfile() else 'directory',
                        'mtime': datetime.fromtimestamp(tarinfo.mtime).strftime('%Y-%m-%d %H:%M:%S') if tarinfo.mtime else 'unknown'
                    })
                    
                    return tarinfo
                
                tar.add(self.config.BITRIX_ROOT, 
                       arcname=os.path.basename(self.config.BITRIX_ROOT),
                       filter=filter_func)
                
                self.log_message(f"Включено файлов/директорий: {included_count}")
                self.log_message(f"Исключено файлов/директорий: {excluded_count}")
            
            size = self._get_human_size(files_backup)
            self.log_message(f"Backup файлов Bitrix создан: {size}")
            return True
            
        except Exception as e:
            self.log_error(f"Ошибка создания backup'а файлов: {e}")
            return False
    
    def backup_system_configs(self) -> bool:
        """Создание backup системных конфигураций"""
        self.log_message("Начинаем backup системных конфигураций...")
        
        try:
            config_backup = os.path.join(self.temp_dir, "system_configs.tar.gz")
            existing_configs = []
            
            # Проверяем какие конфиги существуют
            for config_path in self.config.SYSTEM_CONFIGS:
                if os.path.exists(config_path):
                    existing_configs.append(config_path)
            
            if existing_configs:
                with tarfile.open(config_backup, "w:gz") as tar:
                    for config_path in existing_configs:
                        tar.add(config_path, arcname=config_path)
                
                size = self._get_human_size(config_backup)
                self.log_message(f"Backup системных конфигураций создан: {size}")
            else:
                self.log_message("Системные конфигурации не найдены для backup'а")
            
            return True
            
        except Exception as e:
            self.log_error(f"Ошибка создания backup'а конфигураций: {e}")
            return False
    
    def create_info_file(self) -> bool:
        """Создание информационного файла о backup'е"""
        try:
            info_file = os.path.join(self.temp_dir, "backup_info.txt")
            
            # Получаем информацию о системе
            hostname = subprocess.run(['hostname'], capture_output=True, text=True).stdout.strip()
            host_ip = subprocess.run(['hostname', '-I'], capture_output=True, text=True).stdout.split()[0]
            
            # Версии ПО
            try:
                php_version = subprocess.run(['php', '-r', 'echo PHP_VERSION;'], 
                                           capture_output=True, text=True).stdout
            except:
                php_version = "N/A"
            
            try:
                mysql_version = subprocess.run(['mysql', '-V'], 
                                             capture_output=True, text=True).stdout
            except:
                mysql_version = "N/A"
            
            # Информация о дисках
            try:
                df_output = subprocess.run(['df', '-h'], capture_output=True, text=True).stdout
                disk_info = '\n'.join([line for line in df_output.split('\n') if line.startswith('/dev')])
            except:
                disk_info = "N/A"
            
            # Размеры файлов backup'а
            backup_sizes = []
            for file in os.listdir(self.temp_dir):
                if file != "backup_info.txt":
                    file_path = os.path.join(self.temp_dir, file)
                    size = self._get_human_size(file_path)
                    backup_sizes.append(f"{file}: {size}")
            
            info_content = f"""Bitrix24 Backup Information
===========================
Backup Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Server: {hostname}
Server IP: {host_ip}
OS Version: {self._get_os_version()}
Bitrix Root: {self.config.BITRIX_ROOT}
Database: {self.config.DB_NAME}

Backup Contents:
- Database dump (SQL)
- Bitrix files (excluding cache and temp)
- System configurations
- SSL certificates

System Information:
- PHP Version: {php_version}
- MySQL Version: {mysql_version}

Disk Usage:
{disk_info}

Backup Size Summary:
{chr(10).join(backup_sizes)}
"""
            
            with open(info_file, 'w', encoding='utf-8') as f:
                f.write(info_content)
            
            self.log_message("Информационный файл создан")
            return True
            
        except Exception as e:
            self.log_error(f"Ошибка создания информационного файла: {e}")
            return False
    
    def create_backup_manifest(self) -> bool:
        """Создание манифеста файлов backup'а"""
        try:
            self.log_message("Создаем манифест файлов backup'а...")
            
            # Сортируем файлы по пути для удобства навигации
            included_files_sorted = sorted(self.included_files, key=lambda x: x['path'])
            excluded_files_sorted = sorted(self.excluded_files, key=lambda x: x['path'])
            
            # Статистика
            total_included_size = sum(f['size'] for f in included_files_sorted if f['type'] == 'file')
            total_excluded_size = sum(f['size'] for f in excluded_files_sorted if f['type'] == 'file')
            included_files_count = len([f for f in included_files_sorted if f['type'] == 'file'])
            included_dirs_count = len([f for f in included_files_sorted if f['type'] == 'directory'])
            excluded_files_count = len([f for f in excluded_files_sorted if f['type'] == 'file'])
            excluded_dirs_count = len([f for f in excluded_files_sorted if f['type'] == 'directory'])
            
            # Создаем JSON манифест для программного использования
            manifest_data = {
                "backup_info": {
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "backup_version": "2.0",
                    "bitrix_root": self.config.BITRIX_ROOT,
                    "exclude_patterns": self.config.EXCLUDE_PATTERNS
                },
                "statistics": {
                    "included_files": included_files_count,
                    "included_directories": included_dirs_count,
                    "included_total_size_bytes": total_included_size,
                    "included_total_size_human": self._get_human_size_bytes(total_included_size),
                    "excluded_files": excluded_files_count,
                    "excluded_directories": excluded_dirs_count,
                    "excluded_total_size_bytes": total_excluded_size,
                    "excluded_total_size_human": self._get_human_size_bytes(total_excluded_size)
                },
                "included_files": included_files_sorted,
                "excluded_files": excluded_files_sorted
            }
            
            # Сохраняем JSON манифест
            json_manifest = os.path.join(self.temp_dir, "backup_manifest.json")
            with open(json_manifest, 'w', encoding='utf-8') as f:
                json.dump(manifest_data, f, ensure_ascii=False, indent=2)
            
            # Создаем текстовый отчет для людей
            text_manifest = os.path.join(self.temp_dir, "backup_files_list.txt")
            with open(text_manifest, 'w', encoding='utf-8') as f:
                f.write("МАНИФЕСТ ФАЙЛОВ РЕЗЕРВНОЙ КОПИИ BITRIX24\n")
                f.write("=" * 50 + "\n")
                f.write(f"Дата создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Корневая директория: {self.config.BITRIX_ROOT}\n\n")
                
                f.write("СТАТИСТИКА:\n")
                f.write("-" * 20 + "\n")
                f.write(f"✅ Включено в backup:\n")
                f.write(f"   Файлов: {included_files_count}\n")
                f.write(f"   Директорий: {included_dirs_count}\n")
                f.write(f"   Общий размер: {self._get_human_size_bytes(total_included_size)}\n\n")
                
                f.write(f"❌ Исключено из backup:\n")
                f.write(f"   Файлов: {excluded_files_count}\n")
                f.write(f"   Директорий: {excluded_dirs_count}\n")
                f.write(f"   Общий размер: {self._get_human_size_bytes(total_excluded_size)}\n\n")
                
                f.write("ФАЙЛЫ В BACKUP'Е (отсортировано по пути):\n")
                f.write("-" * 50 + "\n")
                for file_info in included_files_sorted:
                    if file_info['type'] == 'file':
                        size = self._get_human_size_bytes(file_info['size'])
                        f.write(f"📄 {file_info['path']} ({size}) [{file_info['mtime']}]\n")
                    else:
                        f.write(f"📁 {file_info['path']}/\n")
                
                # Добавляем краткую статистику по исключениям
                if excluded_files_sorted:
                    f.write(f"\n\nИСКЛЮЧЕНИЯ ПО ПАТТЕРНАМ (краткая статистика):\n")
                    f.write("-" * 50 + "\n")
                    
                    exclusion_stats = {}
                    for file_info in excluded_files_sorted:
                        pattern = file_info['excluded_by_pattern']
                        if pattern not in exclusion_stats:
                            exclusion_stats[pattern] = {'count': 0, 'size': 0}
                        exclusion_stats[pattern]['count'] += 1
                        exclusion_stats[pattern]['size'] += file_info['size']
                    
                    for pattern, stats in sorted(exclusion_stats.items()):
                        f.write(f"🚫 {pattern}: {stats['count']} файлов/папок, {self._get_human_size_bytes(stats['size'])}\n")
            
            self.log_message(f"Манифест файлов создан: {included_files_count} файлов, {included_dirs_count} директорий")
            return True
            
        except Exception as e:
            self.log_error(f"Ошибка создания манифеста файлов: {e}")
            return False
    
    def _get_human_size_bytes(self, size_bytes: int) -> str:
        """Конвертирует размер в байтах в человеко-читаемый формат"""
        if size_bytes == 0:
            return "0B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f}PB"
    
    def create_final_backup(self) -> Optional[str]:
        """Создание финального архива backup'а"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_name = f"bitrix24_backup_{timestamp}.tar.gz"
            final_backup = os.path.join(self.config.BACKUP_DIR, backup_name)
            
            self.log_message("Создаем финальный архив backup'а...")
            
            with tarfile.open(final_backup, "w:gz") as tar:
                for item in os.listdir(self.temp_dir):
                    item_path = os.path.join(self.temp_dir, item)
                    tar.add(item_path, arcname=item)
            
            backup_size = self._get_human_size(final_backup)
            self.log_message(f"Финальный backup создан: {backup_name} ({backup_size})")
            
            return final_backup
            
        except Exception as e:
            self.log_error(f"Ошибка создания финального архива: {e}")
            return None
    
    def rotate_backups(self):
        """Ротация старых backup'ов"""
        self.log_message("Проверяем количество backup'ов...")
        
        try:
            backup_pattern = "bitrix24_backup_*.tar.gz"
            backup_files = list(Path(self.config.BACKUP_DIR).glob(backup_pattern))
            backup_count = len(backup_files)
            
            if backup_count > self.config.MAX_BACKUPS:
                excess = backup_count - self.config.MAX_BACKUPS
                self.log_message(f"Найдено {backup_count} backup'ов, удаляем {excess} старых...")
                
                # Сортируем по времени модификации (старые первыми)
                backup_files.sort(key=lambda x: x.stat().st_mtime)
                
                for old_backup in backup_files[:excess]:
                    old_backup.unlink()
                    self.log_message(f"Удален старый backup: {old_backup.name}")
            else:
                self.log_message(f"Backup'ов: {backup_count} (максимум: {self.config.MAX_BACKUPS})")
                
        except Exception as e:
            self.log_error(f"Ошибка ротации backup'ов: {e}")
    
    def _get_s3_client(self):
        """Создание S3 клиента для работы с облачным хранилищем backup'ов"""
        if not S3_AVAILABLE:
            raise ImportError("boto3 не установлен. Установите: pip install boto3")
        
        s3_config = self.config.get_s3_params()
        if not s3_config:
            raise ValueError("S3_CONFIG не настроен в конфигурации")
        
        return boto3.client(
            's3',
            endpoint_url=s3_config['endpoint_url'],
            aws_access_key_id=s3_config['access_key'],
            aws_secret_access_key=s3_config['secret_key'],
            region_name='us-east-1'  # Требуется для некоторых S3-совместимых хранилищ
        )
    
    def _get_s3_work_client(self):
        """Создание S3 клиента для работы с файловым хранилищем Bitrix"""
        if not S3_AVAILABLE:
            raise ImportError("boto3 не установлен. Установите: pip install boto3")
        
        s3_work_config = self.config.get_s3_work_storage_params()
        if not s3_work_config:
            raise ValueError("S3_WORK_STORAGE_CONFIG не настроен в конфигурации")
        
        return boto3.client(
            's3',
            endpoint_url=s3_work_config['endpoint_url'],
            aws_access_key_id=s3_work_config['access_key'],
            aws_secret_access_key=s3_work_config['secret_key'],
            region_name='us-east-1'  # Требуется для некоторых S3-совместимых хранилищ
        )
    
    def upload_to_s3(self, backup_file: str) -> bool:
        """Загрузка backup'а в S3 хранилище"""
        if not hasattr(self.config, 'STORAGE_TYPE') or self.config.STORAGE_TYPE != 's3':
            return True  # Пропускаем если S3 не активировано
        
        try:
            self.log_message("Загружаем backup в S3 хранилище...")
            
            s3_client = self._get_s3_client()
            s3_config = self.config.get_s3_params()
            
            # Проверяем существование бакета
            try:
                s3_client.head_bucket(Bucket=s3_config['bucket_name'])
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    self.log_error(f"Бакет {s3_config['bucket_name']} не существует")
                    return False
                else:
                    self.log_error(f"Ошибка доступа к бакету: {e}")
                    return False
            
            # Формируем путь в S3
            backup_name = os.path.basename(backup_file)
            s3_key = f"{s3_config['backup_path']}/{backup_name}"
            
            # Загружаем файл
            file_size = os.path.getsize(backup_file)
            self.log_message(f"Загружаем {backup_name} ({self._get_human_size(backup_file)}) в S3...")
            
            s3_client.upload_file(
                backup_file, 
                s3_config['bucket_name'], 
                s3_key,
                ExtraArgs={
                    'Metadata': {
                        'backup-version': '2.0',
                        'created-timestamp': datetime.now().isoformat(),
                        'server-hostname': self._get_hostname(),
                        'bitrix-root': self.config.BITRIX_ROOT
                    }
                }
            )
            
            self.log_message(f"✅ Backup успешно загружен в S3: s3://{s3_config['bucket_name']}/{s3_key}")
            return True
            
        except NoCredentialsError:
            self.log_error("S3 ошибка: неверные учетные данные")
            return False
        except ClientError as e:
            self.log_error(f"S3 ошибка: {e}")
            return False
        except Exception as e:
            self.log_error(f"Ошибка загрузки в S3: {e}")
            return False
    
    def rotate_s3_backups(self) -> bool:
        """Ротация старых backup'ов в S3"""
        if not hasattr(self.config, 'STORAGE_TYPE') or self.config.STORAGE_TYPE != 's3':
            return True  # Пропускаем если S3 не активировано
        
        try:
            self.log_message("Проверяем количество backup'ов в S3...")
            
            s3_client = self._get_s3_client()
            s3_config = self.config.get_s3_params()
            
            # Получаем список объектов в папке backup'ов
            response = s3_client.list_objects_v2(
                Bucket=s3_config['bucket_name'],
                Prefix=f"{s3_config['backup_path']}/bitrix24_backup_",
                Delimiter='/'
            )
            
            if 'Contents' not in response:
                self.log_message("Backup'ы в S3 не найдены")
                return True
            
            # Сортируем backup'ы по времени последней модификации (старые первыми)
            backup_objects = sorted(response['Contents'], key=lambda x: x['LastModified'])
            backup_count = len(backup_objects)
            max_backups = s3_config.get('max_backups', self.config.MAX_BACKUPS)
            
            if backup_count > max_backups:
                excess = backup_count - max_backups
                self.log_message(f"Найдено {backup_count} backup'ов в S3, удаляем {excess} старых...")
                
                # Удаляем старые backup'ы
                for old_backup in backup_objects[:excess]:
                    s3_client.delete_object(
                        Bucket=s3_config['bucket_name'],
                        Key=old_backup['Key']
                    )
                    backup_name = os.path.basename(old_backup['Key'])
                    self.log_message(f"Удален старый backup из S3: {backup_name}")
            else:
                self.log_message(f"Backup'ов в S3: {backup_count} (максимум: {max_backups})")
            
            return True
            
        except Exception as e:
            self.log_error(f"Ошибка ротации backup'ов в S3: {e}")
            return False
    
    def upload_single_file_to_s3(self, file_path: str) -> bool:
        """Загрузка одного файла в S3 (для тестирования)"""
        try:
            if not os.path.exists(file_path):
                self.log_error(f"Файл не найден: {file_path}")
                return False
            
            self.log_message(f"Загружаем файл в S3: {file_path}")
            
            s3_client = self._get_s3_client()
            s3_config = self.config.get_s3_params()
            
            # Проверяем существование бакета
            try:
                s3_client.head_bucket(Bucket=s3_config['bucket_name'])
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    self.log_error(f"Бакет {s3_config['bucket_name']} не существует")
                    return False
                else:
                    self.log_error(f"Ошибка доступа к бакету: {e}")
                    return False
            
            # Формируем путь в S3
            file_name = os.path.basename(file_path)
            s3_key = f"{s3_config['backup_path']}/{file_name}"
            
            # Получаем информацию о файле
            file_size = os.path.getsize(file_path)
            human_size = self._get_human_size(file_path)
            
            self.log_message(f"Файл: {file_name}")
            self.log_message(f"Размер: {human_size}")
            self.log_message(f"S3 ключ: {s3_key}")
            
            # Загружаем файл
            s3_client.upload_file(
                file_path, 
                s3_config['bucket_name'], 
                s3_key,
                ExtraArgs={
                    'Metadata': {
                        'backup-version': '2.0',
                        'uploaded-timestamp': datetime.now().isoformat(),
                        'server-hostname': self._get_hostname(),
                        'manual-upload': 'true'
                    }
                }
            )
            
            self.log_message(f"✅ Файл успешно загружен в S3: s3://{s3_config['bucket_name']}/{s3_key}")
            return True
            
        except NoCredentialsError:
            self.log_error("S3 ошибка: неверные учетные данные")
            return False
        except ClientError as e:
            self.log_error(f"S3 ошибка: {e}")
            return False
        except Exception as e:
            self.log_error(f"Ошибка загрузки файла в S3: {e}")
            return False

    def backup_s3_files(self) -> bool:
        """Резервное копирование файлов из S3 файлового хранилища Bitrix"""
        # Проверяем нужно ли выполнять backup S3 файлов
        if not hasattr(self.config, 'S3_FILE_BACKUP_ENABLED') or not self.config.S3_FILE_BACKUP_ENABLED:
            self.log_message("Backup S3 файлового хранилища отключен")
            return True
        
        if not hasattr(self.config, 'STORAGE_TYPE') or self.config.STORAGE_TYPE != 's3':
            self.log_message("Backup S3 файлов доступен только при STORAGE_TYPE: s3")
            return True
        
        try:
            self.log_message("========== НАЧИНАЕМ BACKUP S3 ФАЙЛОВОГО ХРАНИЛИЩА ==========")
            
            # Получаем конфигурации для обоих хранилищ
            s3_backup_config = self.config.get_s3_params()
            s3_work_config = self.config.get_s3_work_storage_params()
            
            if not s3_backup_config or not s3_work_config:
                self.log_error("Не настроены конфигурации S3 хранилищ")
                return False
            
            # Создаем клиенты для обоих хранилищ
            s3_backup_client = self._get_s3_client()
            s3_work_client = self._get_s3_work_client()
            
            # Проверяем доступность хранилищ
            if not self._check_s3_connectivity(s3_backup_client, s3_backup_config, "backup") or \
               not self._check_s3_connectivity(s3_work_client, s3_work_config, "work"):
                return False
            
            # Создаем папку для backup'а с timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_folder_path = f"{s3_work_config['backup_folder']}/{timestamp}"
            
            self.log_message(f"Создаем backup S3 файлов в: {backup_folder_path}")
            
            # Получаем статистику исходного хранилища
            source_stats = self._get_s3_storage_stats(s3_work_client, s3_work_config['bucket_name'])
            self.log_message(f"Исходное S3 хранилище: {source_stats['human_size']} ({source_stats['count']} файлов)")
            
            # Выполняем копирование
            copied_count = self._copy_s3_objects(
                s3_work_client, s3_work_config['bucket_name'], "",  # источник
                s3_backup_client, s3_backup_config['bucket_name'], backup_folder_path  # назначение
            )
            
            if copied_count > 0:
                # Проверяем результат
                backup_stats = self._get_s3_storage_stats(
                    s3_backup_client, 
                    s3_backup_config['bucket_name'], 
                    backup_folder_path
                )
                
                self.log_message(f"Создан backup S3 файлов: {backup_stats['human_size']} ({backup_stats['count']} файлов)")
                
                # Проверяем соответствие количества файлов
                if source_stats['count'] == backup_stats['count']:
                    self.log_message("✅ Проверка целостности backup'а S3 файлов пройдена")
                    
                    # Выполняем ротацию старых backup'ов
                    self.rotate_s3_work_backups()
                    
                    self.log_message("========== BACKUP S3 ФАЙЛОВОГО ХРАНИЛИЩА ЗАВЕРШЕН УСПЕШНО ==========")
                    return True
                else:
                    self.log_error(f"❌ Несоответствие количества файлов: исходных {source_stats['count']}, скопированных {backup_stats['count']}")
                    return False
            else:
                self.log_error("Не удалось скопировать файлы из S3 хранилища")
                return False
                
        except Exception as e:
            self.log_error(f"Ошибка backup'а S3 файлов: {e}")
            return False
    
    def _check_s3_connectivity(self, s3_client, s3_config: Dict, storage_type: str) -> bool:
        """Проверка доступности S3 хранилища"""
        try:
            s3_client.head_bucket(Bucket=s3_config['bucket_name'])
            self.log_message(f"✅ S3 {storage_type} хранилище доступно: {s3_config['bucket_name']}")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.log_error(f"❌ S3 {storage_type} бакет не существует: {s3_config['bucket_name']}")
            else:
                self.log_error(f"❌ Ошибка доступа к S3 {storage_type} бакету: {e}")
            return False
        except Exception as e:
            self.log_error(f"❌ Ошибка подключения к S3 {storage_type} хранилищу: {e}")
            return False
    
    def _get_s3_storage_stats(self, s3_client, bucket_name: str, prefix: str = "") -> Dict:
        """Получение статистики S3 хранилища"""
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            total_size = 0
            total_count = 0
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_size += obj['Size']
                        total_count += 1
            
            return {
                'size': total_size,
                'count': total_count,
                'human_size': self._get_human_size_bytes(total_size)
            }
        except Exception as e:
            self.log_error(f"Ошибка получения статистики S3: {e}")
            return {'size': 0, 'count': 0, 'human_size': '0B'}
    
    def _copy_s3_objects(self, source_client, source_bucket: str, source_prefix: str,
                         target_client, target_bucket: str, target_prefix: str) -> int:
        """Копирование объектов между S3 хранилищами"""
        try:
            paginator = source_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=source_bucket, Prefix=source_prefix)
            
            copied_count = 0
            
            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                
                for obj in page['Contents']:
                    source_key = obj['Key']
                    target_key = f"{target_prefix}/{source_key}" if target_prefix else source_key
                    
                    try:
                        # Формируем URL источника для copy_object
                        copy_source = {
                            'Bucket': source_bucket,
                            'Key': source_key
                        }
                        
                        # Копируем объект
                        target_client.copy_object(
                            CopySource=copy_source,
                            Bucket=target_bucket,
                            Key=target_key,
                            MetadataDirective='COPY'
                        )
                        
                        copied_count += 1
                        
                        # Логируем прогресс каждые 100 файлов
                        if copied_count % 100 == 0:
                            self.log_message(f"Скопировано файлов: {copied_count}")
                            
                    except Exception as e:
                        self.log_error(f"Ошибка копирования объекта {source_key}: {e}")
                        continue
            
            self.log_message(f"Всего скопировано объектов: {copied_count}")
            return copied_count
            
        except Exception as e:
            self.log_error(f"Ошибка копирования S3 объектов: {e}")
            return 0
    
    def rotate_s3_work_backups(self) -> bool:
        """Ротация старых backup'ов S3 файлового хранилища"""
        try:
            self.log_message("Проверяем количество backup'ов S3 файлов...")
            
            s3_backup_client = self._get_s3_client()
            s3_backup_config = self.config.get_s3_params()
            s3_work_config = self.config.get_s3_work_storage_params()
            
            if not s3_backup_config or not s3_work_config:
                return True
            
            # Получаем список папок backup'ов
            response = s3_backup_client.list_objects_v2(
                Bucket=s3_backup_config['bucket_name'],
                Prefix=f"{s3_work_config['backup_folder']}/",
                Delimiter='/'
            )
            
            # Извлекаем имена папок (timestamp'ы)
            backup_folders = []
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    folder_name = prefix['Prefix'].rstrip('/').split('/')[-1]
                    # Проверяем что это папка с timestamp (формат YYYYMMDD_HHMMSS)
                    if len(folder_name) == 15 and '_' in folder_name:
                        backup_folders.append(folder_name)
            
            backup_count = len(backup_folders)
            max_backups = s3_work_config['max_backups']
            
            if backup_count > max_backups:
                excess = backup_count - max_backups
                self.log_message(f"Найдено {backup_count} backup'ов S3 файлов, удаляем {excess} старых...")
                
                # Сортируем по timestamp (старые первыми)
                backup_folders.sort()
                
                # Удаляем старые backup'ы
                for old_folder in backup_folders[:excess]:
                    folder_prefix = f"{s3_work_config['backup_folder']}/{old_folder}/"
                    
                    # Получаем все объекты в папке
                    objects_to_delete = []
                    paginator = s3_backup_client.get_paginator('list_objects_v2')
                    page_iterator = paginator.paginate(
                        Bucket=s3_backup_config['bucket_name'],
                        Prefix=folder_prefix
                    )
                    
                    for page in page_iterator:
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                objects_to_delete.append({'Key': obj['Key']})
                    
                    # Удаляем объекты пакетами (максимум 1000 за раз)
                    if objects_to_delete:
                        for i in range(0, len(objects_to_delete), 1000):
                            batch = objects_to_delete[i:i+1000]
                            s3_backup_client.delete_objects(
                                Bucket=s3_backup_config['bucket_name'],
                                Delete={'Objects': batch}
                            )
                        
                        self.log_message(f"Удален старый backup S3 файлов: {old_folder} ({len(objects_to_delete)} объектов)")
            else:
                self.log_message(f"Backup'ов S3 файлов: {backup_count} (максимум: {max_backups})")
            
            return True
            
        except Exception as e:
            self.log_error(f"Ошибка ротации backup'ов S3 файлов: {e}")
            return False

    def manage_backup_storage(self, backup_file: str) -> bool:
        """Управление хранением backup'а в зависимости от настроек"""
        success = True
        
        # Локальная ротация (всегда выполняется для локальных файлов)
        if not hasattr(self.config, 'STORAGE_TYPE') or self.config.STORAGE_TYPE == 'local':
            self.rotate_backups()
        
        # S3 операции (если настроено)
        if hasattr(self.config, 'STORAGE_TYPE') and self.config.STORAGE_TYPE == 's3':
            # Загружаем в S3
            if not self.upload_to_s3(backup_file):
                success = False
            
            # Ротация в S3
            if not self.rotate_s3_backups():
                success = False
            
            # Удаляем локальный файл после успешной загрузки в S3 (опционально)
            if success:
                s3_config = self.config.get_s3_params()
                try:
                    if s3_config and s3_config.get('delete_local_after_upload', False):
                        os.remove(backup_file)
                        self.log_message(f"Локальный backup удален после загрузки в S3: {os.path.basename(backup_file)}")
                except Exception as e:
                    self.log_error(f"Ошибка удаления локального backup'а: {e}")
        
        return success
    
    def send_notification(self, status: str, backup_file: str = ""):
        """Отправка email уведомления"""
        try:
            if status == "success":
                subject = "✅ Bitrix24 Backup - Успешно"
                backup_size = self._get_human_size(backup_file) if backup_file else "N/A"
                
                # Определяем тип хранения
                storage_info = ""
                s3_files_info = ""
                
                if hasattr(self.config, 'STORAGE_TYPE') and self.config.STORAGE_TYPE == 's3':
                    s3_config = self.config.get_s3_params()
                    backup_name = os.path.basename(backup_file) if backup_file else 'N/A'
                    s3_path = f"s3://{s3_config['bucket_name']}/{s3_config['backup_path']}/{backup_name}"
                    storage_info = f"""
Хранилище: S3 облачное хранилище
S3 путь: {s3_path}
Локальный путь: {backup_file}"""
                    
                    # Добавляем информацию о backup'е S3 файлов если включен
                    if hasattr(self.config, 'S3_FILE_BACKUP_ENABLED') and self.config.S3_FILE_BACKUP_ENABLED:
                        s3_work_config = self.config.get_s3_work_storage_params()
                        if s3_work_config:
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            s3_files_path = f"s3://{s3_config['bucket_name']}/{s3_work_config['backup_folder']}/{timestamp}/"
                            s3_files_info = f"""
S3 файловое хранилище: ВКЛЮЧЕНО
Источник: s3://{s3_work_config['bucket_name']}/
Backup S3 файлов: {s3_files_path}"""
                else:
                    storage_info = f"""
Хранилище: Локальное файловое хранилище  
Путь к backup: {backup_file}"""

                message = f"""Резервное копирование завершено успешно.

Backup файл: {os.path.basename(backup_file) if backup_file else 'N/A'}
Размер: {backup_size}
Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Сервер: {self._get_hostname()} ({self._get_host_ip()}){storage_info}{s3_files_info}
Лог выполнения: {os.path.join(self.config.LOG_DIR, 'bitrix_backup.log')}

Система: Bitrix24 на BitrixVM 9.0.7"""
            else:
                subject = "❌ Bitrix24 Backup - Ошибка"
                message = f"""Произошла ошибка при создании резервной копии.

Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Сервер: {self._get_hostname()} ({self._get_host_ip()})

Проверьте лог выполнения: {os.path.join(self.config.LOG_DIR, 'bitrix_backup.log')}

Система: Bitrix24 на BitrixVM 9.0.7"""
            
            # Пытаемся отправить через SMTP
            if self._send_smtp_email(subject, message):
                self.log_message(f"✅ Email уведомление отправлено успешно: {subject}")
            else:
                # Fallback - отправка через PHP mail()
                if self._send_php_email(subject, message):
                    self.log_message(f"✅ Email уведомление отправлено через PHP: {subject}")
                else:
                    self.log_message(f"❌ Ошибка отправки email уведомления")
            
            # Локальное уведомление (всегда работает как резерв)
            self._save_local_notification(subject, message)
            
        except Exception as e:
            self.log_error(f"Ошибка отправки уведомления: {e}")
    
    def _send_smtp_email(self, subject: str, message: str) -> bool:
        """Отправка email через SMTP"""
        try:
            # Получаем SMTP параметры из конфигурационного файла
            smtp_params = self.config.get_smtp_params()
            if not smtp_params:
                self.log_message("SMTP не настроен, используется PHP mail()")
                return False
            
            if not all([self.config.EMAIL_FROM, self.config.EMAIL_TO]):
                self.log_error("EMAIL_FROM или EMAIL_TO не настроены")
                return False
            
            msg = MIMEMultipart()
            msg['From'] = self.config.EMAIL_FROM
            msg['To'] = self.config.EMAIL_TO
            msg['Subject'] = subject
            msg.attach(MIMEText(message, 'plain', 'utf-8'))
            
            # Определяем тип подключения
            smtp_port = int(smtp_params['port'])
            use_tls = smtp_params['use_tls']
            
            if smtp_port == 465:
                # SSL подключение
                with smtplib.SMTP_SSL(smtp_params['server'], smtp_port) as server:
                    server.login(smtp_params['username'], smtp_params['password'])
                    server.send_message(msg)
            else:
                # TLS подключение
                with smtplib.SMTP(smtp_params['server'], smtp_port) as server:
                    if use_tls:
                        server.starttls()
                    server.login(smtp_params['username'], smtp_params['password'])
                    server.send_message(msg)
            
            self.log_message(f"Email отправлен через SMTP: {smtp_params['server']}")
            return True
            
        except Exception as e:
            self.log_error(f"SMTP ошибка: {e}")
            return False
    
    def _send_php_email(self, subject: str, message: str) -> bool:
        """Отправка email через PHP mail() (fallback)"""
        try:
            php_script = f"""<?php
$to = '{self.config.EMAIL_TO}';
$subject = '{subject}';
$message = '{message}';
$headers = array(
    'From: {self.config.EMAIL_FROM}',
    'Reply-To: {self.config.EMAIL_FROM}',
    'X-Mailer: Bitrix24 Backup System',
    'Content-Type: text/plain; charset=UTF-8'
);

$result = mail($to, $subject, $message, implode("\\r\\n", $headers));
echo $result ? 'SUCCESS' : 'FAILED';
?>"""
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.php', delete=False) as f:
                f.write(php_script)
                php_file = f.name
            
            try:
                result = subprocess.run(['sudo', '-u', 'bitrix', 'php', php_file], 
                                      capture_output=True, text=True)
                return result.stdout.strip() == 'SUCCESS' and result.returncode == 0
            finally:
                os.unlink(php_file)
                
        except Exception as e:
            self.log_error(f"PHP mail ошибка: {e}")
            return False
    
    def _save_local_notification(self, subject: str, message: str):
        """Логирование уведомления в системный лог"""
        # Записываем уведомление в системный лог
        self.log_message(f"EMAIL NOTIFICATION: {subject}")
        for line in message.split('\n'):
            if line.strip():
                self.log_message(f"  {line}")
    
    def _get_human_size(self, file_path: str) -> str:
        """Получение размера файла в человекочитаемом формате"""
        try:
            size = os.path.getsize(file_path)
            for unit in ['B', 'KB', 'MB', 'GB']:
                if size < 1024.0:
                    return f"{size:.1f}{unit}"
                size /= 1024.0
            return f"{size:.1f}TB"
        except:
            return "N/A"
    
    def _get_hostname(self) -> str:
        """Получение имени хоста"""
        try:
            return subprocess.run(['hostname'], capture_output=True, text=True).stdout.strip()
        except:
            return "unknown"
    
    def _get_host_ip(self) -> str:
        """Получение IP адреса хоста"""
        try:
            result = subprocess.run(['hostname', '-I'], capture_output=True, text=True)
            return result.stdout.split()[0]
        except:
            return "unknown"
    
    def _get_os_version(self) -> str:
        """Получение версии ОС"""
        try:
            with open('/etc/os-release', 'r') as f:
                for line in f:
                    if line.startswith('PRETTY_NAME='):
                        return line.split('=', 1)[1].strip().strip('"')
        except:
            pass
        return "unknown"
    
    def run_backup(self) -> bool:
        """Основная функция выполнения backup'а"""
        self.log_message("=" * 10 + " НАЧАЛО РЕЗЕРВНОГО КОПИРОВАНИЯ " + "=" * 10)
        self.log_message("Bitrix24 Backup Script (Python) v2.0")
        
        try:
            # Создаем необходимые директории
            os.makedirs(self.config.BACKUP_DIR, exist_ok=True)
            self.temp_dir = tempfile.mkdtemp(prefix='bitrix_backup_')
            
            # Проверки
            if not self.check_disk_space():
                return False
            
            # Выполняем backup'ы системы и БД
            if (self.backup_database() and 
                self.backup_files() and 
                self.backup_system_configs()):
                
                self.create_info_file()
                self.create_backup_manifest()
                
                final_backup = self.create_final_backup()
                if final_backup:
                    self.cleanup_temp()
                    
                    # Управление хранением backup'а (локально или S3)
                    storage_success = self.manage_backup_storage(final_backup)
                    
                    # Резервное копирование S3 файлового хранилища (если настроено)
                    s3_files_success = True
                    if storage_success:  # Выполняем только после успешного backup'а системы
                        s3_files_success = self.backup_s3_files()
                    
                    if storage_success and s3_files_success:
                        self.log_message("=" * 10 + " РЕЗЕРВНОЕ КОПИРОВАНИЕ ЗАВЕРШЕНО УСПЕШНО " + "=" * 10)
                        self.send_notification("success", final_backup)
                        return True
                    else:
                        if not storage_success:
                            self.log_error("Ошибка управления хранением backup'а")
                        if not s3_files_success:
                            self.log_error("Ошибка backup'а S3 файлового хранилища")
                else:
                    self.log_error("Ошибка создания финального backup'а")
            else:
                self.log_error("Ошибка в процессе резервного копирования")
            
        except Exception as e:
            self.log_error(f"Критическая ошибка: {e}")
        finally:
            self.cleanup_temp()
        
        self.log_message("=" * 10 + " РЕЗЕРВНОЕ КОПИРОВАНИЕ ЗАВЕРШЕНО С ОШИБКАМИ " + "=" * 10)
        self.send_notification("error")
        return False


def main():
    """Точка входа в приложение"""
    parser = argparse.ArgumentParser(
        description='Bitrix24 Backup System (Python) v2.0',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Примеры использования:
  %(prog)s                                    # Полный backup
  %(prog)s --s3-only-file-transfer file.tar.gz  # Загрузка файла в S3
        '''
    )
    
    parser.add_argument(
        '--s3-only-file-transfer',
        metavar='FILE_PATH',
        help='Загрузить указанный файл в S3 хранилище (без создания backup\'а)'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='Bitrix24 Backup System v2.0'
    )
    
    args = parser.parse_args()
    
    try:
        backup = BitrixBackup()
        
        # Режим загрузки одного файла в S3
        if args.s3_only_file_transfer:
            print(f"Режим загрузки файла в S3: {args.s3_only_file_transfer}")
            success = backup.upload_single_file_to_s3(args.s3_only_file_transfer)
            if success:
                print("✅ Файл успешно загружен в S3")
            else:
                print("❌ Ошибка загрузки файла в S3")
            sys.exit(0 if success else 1)
        
        # Обычный режим - полный backup
        success = backup.run_backup()
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()