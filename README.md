# Bitrix24 Backup System v2.0

Продвинутая система резервного копирования для Bitrix24 с поддержкой S3 облачного хранения, написанная на Python. Поддерживает локальное и облачное хранение backup'ов, резервное копирование S3 файлового хранилища Bitrix24.

## ✨ Основные возможности

🗄️ **Полное резервное копирование**
- База данных MySQL (с безопасной аутентификацией)
- Файлы Bitrix24 (с умными исключениями - 42 паттерна)
- Системные конфигурации (Nginx, Apache, PHP, SSL)
- S3 файловое хранилище Bitrix24 (пользовательские файлы)

☁️ **S3 интеграция**
- Автоматическая загрузка backup'ов в S3-совместимые хранилища
- Поддержка любых S3-провайдеров (AWS, Selectel, Beget, etc.)
- Резервное копирование S3 файлового хранилища между бакетами
- Автоматическая ротация backup'ов в облаке

📧 **Уведомления**
- Email уведомления о статусе backup'а
- SMTP поддержка + fallback через PHP mail()
- Детальная информация о размерах и статистике

📊 **Мониторинг и отчетность**
- Детальные логи с ротацией
- JSON манифест всех файлов backup'а
- Статистика исключенных/включенных файлов
- Проверка целостности и места на диске

🔄 **Автоматизация**
- Ротация старых backup'ов (локально и в S3)
- Поддержка cron для автоматического запуска
- Командная строка с различными режимами работы

## 🚀 Быстрая установка

### 1. Подготовка
```bash
# Клонируем проект
git clone <repository-url> bitrix-backup
cd bitrix-backup

# ВАРИАНТ 1: Установка в глобальное окружение
pip3 install -r requirements.txt

# ВАРИАНТ 2: Установка в виртуальную среду (рекомендуется)
python3 -m venv .venv
source .venv/bin/activate  # или ./activate.sh
pip install -r requirements.txt
```

### 2. Настройка конфигурации
```bash
# Копируем пример конфигурации
cp config.yaml.example config.yaml

# Редактируем config.yaml под ваши нужды
nano config.yaml

# Настраиваем подключение к MySQL (создаем /root/.my.cnf)
nano /root/.my.cnf
```

**Создайте `/root/.my.cnf` для безопасного подключения к MySQL:**
```ini
[client]
user=root
password=your_mysql_password_here
socket=/var/lib/mysqld/mysqld.sock

# Альтернативно для TCP подключения:
# user=bitrix0
# password=your_mysql_password_here
# host=localhost
# port=3306
```

**Устанавливаем правильные права на файлы:**
```bash
# Права для MySQL конфигурации
sudo chmod 600 /root/.my.cnf
sudo chown root:root /root/.my.cnf
```

### 3. Настройка конфигурации S3 (опционально)

Если хотите использовать S3 облачное хранение, настройте параметры в `config.yaml`:

```yaml
# Настройки хранения backup'ов
STORAGE_TYPE: "s3"  # "local" или "s3"

# S3-совместимое хранилище backup'ов (куда сохраняем)
S3_CONFIG:
  endpoint_url: "https://s3.amazonaws.com"  # или другой S3 провайдер
  bucket_name: "your-backup-bucket"
  access_key: "YOUR_ACCESS_KEY"
  secret_key: "YOUR_SECRET_KEY"
  backup_path: "daily-bitrix-backups"
  max_backups: 5
  delete_local_after_upload: true

# Резервное копирование S3 файлового хранилища Bitrix24
S3_FILE_BACKUP_ENABLED: true

# S3 файловое хранилище Bitrix24 (откуда копируем файлы)
S3_WORK_STORAGE_CONFIG:
  endpoint_url: "https://s3.amazonaws.com"
  bucket_name: "your-work-files-bucket" 
  access_key: "WORK_ACCESS_KEY"
  secret_key: "WORK_SECRET_KEY"
  backup_folder: "s3-work-file-storage"
  max_backups: 5
```

### 4. Запуск

**Глобальное окружение:**
```bash
# Создание backup'а
python3 scripts/bitrix_backup.py

# Загрузка отдельного файла в S3 (тестирование)
python3 scripts/bitrix_backup.py --s3-only-file-transfer /path/to/file.tar.gz
```

**Виртуальная среда:**
```bash
# Активация виртуальной среды (если не активна)
source .venv/bin/activate

# Создание backup'а  
python scripts/bitrix_backup.py

# Деактивация (опционально)
deactivate
```

## 📋 Использование

### Основные команды
```bash
# Полный backup (система + БД + файлы + S3 файлы если настроено)
python3 scripts/bitrix_backup.py

# Загрузка файла в S3 (для тестирования S3 подключения)
python3 scripts/bitrix_backup.py --s3-only-file-transfer /backup/daily/test.tar.gz

# Просмотр справки
python3 scripts/bitrix_backup.py --help

# Версия системы
python3 scripts/bitrix_backup.py --version
```

### С виртуальной средой (рекомендуется)
```bash
# Быстрая активация и запуск
./activate.sh
python scripts/bitrix_backup.py
```

### Настройка автозапуска
```bash
# Добавить в cron для ежедневного запуска в 2:00
echo "0 2 * * * cd /path/to/bitrix-backup && python3 scripts/bitrix_backup.py" | crontab -

# Для виртуальной среды
echo "0 2 * * * cd /path/to/bitrix-backup && .venv/bin/python scripts/bitrix_backup.py" | crontab -
```

### Мониторинг
```bash
# Просмотр логов backup'а
tail -f /backup/logs/bitrix_backup.log

# Проверка места на диске
df -h /backup

# Список локальных backup'ов
ls -lah /backup/daily/

# Статистика последнего backup'а (JSON манифест)
cat /backup/daily/bitrix24_backup_YYYYMMDD_HHMMSS.tar.gz # извлечь backup_manifest.json
```

## ⚙️ Конфигурация

Основной файл конфигурации: `config.yaml` - использует YAML формат для удобного редактирования.

### Основные секции конфигурации:

#### 📁 Пути и директории
```yaml
BACKUP_DIR: "/backup/daily"           # Где хранить backup'ы
BITRIX_ROOT: "/home/bitrix/www"       # Путь к Bitrix24
LOG_DIR: "/backup/logs"               # Директория логов
```

#### 🗄️ База данных
```yaml
DB_NAME: "sitemanager"                # Имя БД Bitrix24
MYSQL_CONFIG: "/root/.my.cnf"         # Путь к .my.cnf (безопасная аутентификация)
```

#### ☁️ S3 хранилище backup'ов
```yaml
STORAGE_TYPE: "s3"                    # "local" или "s3"
S3_CONFIG:
  endpoint_url: "https://s3.amazonaws.com"  # S3 провайдер
  bucket_name: "backup-bucket"         # Бакет для backup'ов
  access_key: "YOUR_ACCESS_KEY"
  secret_key: "YOUR_SECRET_KEY"
  backup_path: "daily-bitrix-backups"  # Папка в бакете
  max_backups: 5                       # Количество backup'ов
  delete_local_after_upload: true      # Удалять локальные backup'ы
```

#### 📁 S3 файловое хранилище Bitrix24
```yaml
S3_FILE_BACKUP_ENABLED: true          # Включить backup пользовательских файлов
S3_WORK_STORAGE_CONFIG:
  endpoint_url: "https://s3.amazonaws.com"
  bucket_name: "work-files-bucket"     # Бакет с файлами Bitrix
  access_key: "WORK_ACCESS_KEY" 
  secret_key: "WORK_SECRET_KEY"
  backup_folder: "s3-work-file-storage" # Папка для backup'ов файлов
  max_backups: 5
```

#### 📧 Email уведомления
```yaml
EMAIL_ENABLED: true
EMAIL_TO: "admin@your-domain.com"     # Получатель уведомлений
EMAIL_FROM: "no-reply@your-domain.com" # Отправитель

SMTP_CONFIG:                          # Опционально (иначе PHP mail)
  server: "smtp.gmail.com"
  port: 587
  username: "your-email@gmail.com"
  password: "your-password"
  use_tls: true
```

#### 📊 Логирование
```yaml
LOG_LEVEL: "INFO"                     # DEBUG, INFO, WARNING, ERROR
LOG_MAX_SIZE_MB: 10                   # Размер лог-файла
LOG_BACKUP_COUNT: 5                   # Количество архивных логов
```

#### 🚫 Исключения из backup'а (42 паттерна)
```yaml
EXCLUDE_PATTERNS:
  # Кэш файлы
  - "bitrix/cache/"
  - "local/cache/"
  - "upload/resize_cache/"
  
  # Временные файлы
  - "bitrix/tmp/"
  - "*.tmp"
  - "*.log"
  
  # Git репозиторий
  - ".git/"
  
  # И еще 35+ паттернов...
```

## 🔧 Что включает backup

### 📦 Состав backup'а:
1. **База данных** - полный дамп MySQL с сохранением структуры и данных
2. **Файлы Bitrix24** - все файлы кроме кэша, логов и временных файлов
3. **Системные конфигурации** - Nginx, Apache, PHP, MySQL, SSL сертификаты 
4. **S3 файловое хранилище** - резервная копия пользовательских файлов (если включено)
5. **Информационные файлы**:
   - `backup_info.txt` - информация о системе и backup'е
   - `backup_manifest.json` - JSON манифест всех файлов
   - `backup_files_list.txt` - читаемый список включенных/исключенных файлов

### 📊 Создаваемые файлы в архиве:
```
bitrix24_backup_20240102_143022.tar.gz
├── database_sitemanager.sql      # Дамп БД
├── bitrix_files.tar.gz           # Файлы Bitrix (сжатые)
├── system_configs.tar.gz         # Системные конфигурации
├── backup_info.txt               # Информация о backup'е
├── backup_manifest.json          # JSON манифест файлов  
└── backup_files_list.txt         # Список файлов (человеко-читаемый)
```

## 📁 Структура проекта

```
bitrix-backup/
├── scripts/
│   ├── bitrix_backup.py      # Основной скрипт backup'а (v2.0)
│   └── config.py             # Класс загрузки конфигурации (YAML + MySQL)
├── logs/                     # Логи системы (создается автоматически)
├── daily/                    # Локальные backup'ы (создается автоматически)
├── temp/                     # Временные файлы (создается автоматически)
├── .venv/                    # Виртуальная среда Python (создается)
├── config.yaml               # Главная конфигурация (YAML)
├── config.yaml.example       # Пример конфигурации
├── requirements.txt          # Python зависимости
├── test_s3_connection.py     # Тест S3 подключения
├── activate.sh               # Скрипт активации виртуальной среды
├── .gitignore                # Исключения для Git (безопасность)
└── README.md                 # Документация
```

## 🛠️ Дополнительные утилиты

### 🔧 `test_s3_connection.py` - Тестирование S3
Полезная утилита для проверки подключения к S3 хранилищам перед настройкой backup'а:

```bash
# Тест S3 подключения
python test_s3_connection.py
```

**Возможности:**
- Проверка доступности S3 эндпоинтов
- Тестирование аутентификации (access_key/secret_key)
- Проверка прав доступа к бакетам
- Валидация S3 конфигурации из config.yaml

## 🐍 Работа с виртуальной средой

### Преимущества виртуальной среды
✅ **Изоляция зависимостей** - не конфликтует с системными пакетами  
✅ **Контроль версий** - точные версии пакетов для проекта  
✅ **Безопасность** - изолированное окружение  
✅ **Переносимость** - одинаковое окружение на разных серверах  

### Команды для работы с .venv

```bash
# Создание виртуальной среды (один раз)
python3 -m venv .venv

# Активация
source .venv/bin/activate
# ИЛИ используйте удобный скрипт:
./activate.sh

# Проверка активации
which python   # должно показать .venv/bin/python
pip list        # показать установленные пакеты

# Установка зависимостей
pip install -r requirements.txt

# Обновление зависимостей
pip install -r requirements.txt --upgrade

# Создание файла зависимостей (если добавили новые пакеты)
pip freeze > requirements-freeze.txt

# Деактивация
deactivate
```

### Автоматизация с cron и виртуальной средой
```bash
# Для cron нужно указать полный путь к Python из виртуальной среды
echo "0 2 * * * cd /path/to/bitrix-backup && .venv/bin/python scripts/bitrix_backup.py" | crontab -
```

## 🛠️ Устранение неполадок

### Проблемы с базой данных
```bash
# Тест подключения через .my.cnf
mysql --defaults-file=/root/.my.cnf sitemanager -e "SELECT 1"

# Проверка содержимого .my.cnf
sudo cat /root/.my.cnf

# Проверка прав на файл (должно быть 600)
ls -la /root/.my.cnf
```

### Проблемы с S3
```bash
# Тест S3 подключения
python test_s3_connection.py

# Проверка S3 конфигурации в config.yaml
grep -A 10 "S3_CONFIG:" config.yaml

# Тестовая загрузка файла
python scripts/bitrix_backup.py --s3-only-file-transfer /tmp/test.txt

# Проверка доступности S3 эндпоинта
curl -I https://s3.amazonaws.com
# Или для других провайдеров:
curl -I https://s3.ru1.storage.beget.cloud
```

### Проблемы с SMTP
```bash
# Проверка SMTP настроек в config.yaml
grep -A 10 "SMTP_CONFIG:" config.yaml

# Тест SMTP подключения напрямую
python3 -c "
from scripts.config import BackupConfig
import smtplib
config = BackupConfig()
smtp_params = config.get_smtp_params()
if smtp_params:
    try:
        if int(smtp_params['port']) == 465:
            server = smtplib.SMTP_SSL(smtp_params['server'], int(smtp_params['port']))
        else:
            server = smtplib.SMTP(smtp_params['server'], int(smtp_params['port']))
            if smtp_params['use_tls']: server.starttls()
        server.login(smtp_params['username'], smtp_params['password'])
        print('✅ SMTP подключение успешно!')
        server.quit()
    except Exception as e:
        print(f'❌ SMTP ошибка: {e}')
else:
    print('❌ SMTP не настроен')
"
```

### Проверка логов и статистики
```bash
# Основные логи backup'а
tail -f /backup/logs/bitrix_backup.log

# Поиск ошибок
grep -i error /backup/logs/bitrix_backup.log

# Поиск S3 операций
grep -i s3 /backup/logs/bitrix_backup.log

# Проверка последних backup'ов
ls -la /backup/daily/

# Извлечение манифеста последнего backup'а
cd /backup/daily
tar -tzf bitrix24_backup_*.tar.gz | grep manifest
tar -xzf bitrix24_backup_*.tar.gz backup_manifest.json
cat backup_manifest.json | python -m json.tool
```

### Проверка производительности
```bash
# Место на диске
df -h /backup

# Размеры backup'ов
du -sh /backup/daily/*

# Время выполнения последнего backup'а
grep "НАЧАЛО\|ЗАВЕРШЕНО" /backup/logs/bitrix_backup.log | tail -2
```

## 🚀 Преимущества системы v2.0

### 🎯 Основные преимущества
✅ **S3 интеграция** - полная поддержка облачного хранения с любыми S3-провайдерами  
✅ **Двойной backup S3 файлов** - резервирование пользовательских файлов между S3 хранилищами  
✅ **Умная фильтрация** - 42 паттерна исключений для оптимального размера backup'а  
✅ **Безопасность** - никаких паролей в коде, безопасно для публикации в Git  
✅ **YAML конфигурация** - понятный и гибкий формат настроек  
✅ **Детальные отчеты** - JSON манифест + человеко-читаемые списки файлов  
✅ **Ротация backup'ов** - автоматическое управление локальными и облачными backup'ами  
✅ **Email уведомления** - SMTP + fallback через PHP mail()  

### 🔧 Технические преимущества  
✅ **Виртуальная среда** - изоляция зависимостей, контроль версий  
✅ **Логирование с ротацией** - подробные логи с автоматической ротацией  
✅ **Командная строка** - поддержка различных режимов работы  
✅ **Проверка целостности** - валидация backup'ов и подсчет статистики  
✅ **Минимум зависимостей** - `pip install -r requirements.txt`  
✅ **Cron ready** - готово для автоматического запуска  

### 💾 Экономия места и времени
- **Git исключение** экономит ~1.2GB (история, объекты, индекс)
- **Source maps исключение** экономит ~15-20MB (JS/CSS карты)
- **Cache исключение** экономит ~500MB-2GB (кэш файлы)
- **Умная компрессия** - tar.gz для максимального сжатия

---

**🎉 Готово к production использованию! Добавьте в cron для автоматического запуска.**
