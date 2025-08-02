#!/usr/bin/env python3
"""
Анализатор размеров файлов для резервного копирования Bitrix24
Собирает информацию о файлах, которые попадают в backup с учетом исключений
"""

import os
import json
import fnmatch
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from config import BackupConfig


class BackupSizeAnalyzer:
    """Анализатор размеров файлов для резервного копирования"""
    
    def __init__(self):
        """Инициализация с загрузкой конфигурации"""
        self.config = BackupConfig()
        self.total_files = 0
        self.total_size = 0
        self.excluded_files = 0
        self.excluded_size = 0
        
    def _should_exclude(self, file_path: str, pattern: str) -> bool:
        """
        Проверяет, должен ли файл быть исключен на основе паттерна
        (Копия логики из bitrix_backup.py)
        
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
    
    def _get_human_size(self, size_bytes: int) -> str:
        """Конвертирует размер в байтах в человеко-читаемый формат"""
        if size_bytes == 0:
            return "0B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        size_bytes = float(size_bytes)
        
        i = 0
        while size_bytes >= 1024.0 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.1f}{size_names[i]}"
    
    def _analyze_directory(self, dir_path: str, base_path: str) -> Dict[str, Any]:
        """
        Анализирует директорию и возвращает информацию о размерах
        
        Args:
            dir_path: Полный путь к директории
            base_path: Базовый путь (корень Bitrix)
            
        Returns:
            Dict с информацией о размерах
        """
        result = {
            "type": "directory",
            "name": os.path.basename(dir_path),
            "full_path": dir_path,
            "relative_path": os.path.relpath(dir_path, base_path),
            "total_size_bytes": 0,
            "total_size_human": "0B",
            "included_size_bytes": 0,
            "included_size_human": "0B", 
            "excluded_size_bytes": 0,
            "excluded_size_human": "0B",
            "files_count": 0,
            "included_files_count": 0,
            "excluded_files_count": 0,
            "subdirectories": {},
            "files": []
        }
        
        try:
            items = sorted(os.listdir(dir_path))
        except PermissionError:
            result["error"] = "Permission denied"
            return result
        except Exception as e:
            result["error"] = str(e)
            return result
        
        for item in items:
            item_path = os.path.join(dir_path, item)
            relative_path = os.path.relpath(item_path, base_path)
            
            if os.path.isfile(item_path):
                try:
                    file_size = os.path.getsize(item_path)
                    self.total_files += 1
                    self.total_size += file_size
                    result["files_count"] += 1
                    result["total_size_bytes"] += file_size
                    
                    # Проверяем исключения
                    is_excluded = False
                    excluded_by_pattern = None
                    
                    for pattern in self.config.EXCLUDE_PATTERNS:
                        if self._should_exclude(relative_path, pattern):
                            is_excluded = True
                            excluded_by_pattern = pattern
                            break
                    
                    file_info = {
                        "type": "file",
                        "name": item,
                        "size_bytes": file_size,
                        "size_human": self._get_human_size(file_size),
                        "relative_path": relative_path,
                        "included": not is_excluded,
                        "excluded_by_pattern": excluded_by_pattern
                    }
                    
                    if is_excluded:
                        self.excluded_files += 1
                        self.excluded_size += file_size
                        result["excluded_files_count"] += 1
                        result["excluded_size_bytes"] += file_size
                    else:
                        result["included_files_count"] += 1
                        result["included_size_bytes"] += file_size
                    
                    result["files"].append(file_info)
                    
                except Exception as e:
                    file_info = {
                        "type": "file",
                        "name": item,
                        "error": str(e),
                        "relative_path": relative_path
                    }
                    result["files"].append(file_info)
                    
            elif os.path.isdir(item_path):
                # Рекурсивно анализируем поддиректорию
                subdir_result = self._analyze_directory(item_path, base_path)
                result["subdirectories"][item] = subdir_result
                
                # Суммируем размеры из поддиректории
                result["total_size_bytes"] += subdir_result["total_size_bytes"]
                result["included_size_bytes"] += subdir_result["included_size_bytes"]
                result["excluded_size_bytes"] += subdir_result["excluded_size_bytes"]
                result["files_count"] += subdir_result["files_count"]
                result["included_files_count"] += subdir_result["included_files_count"]
                result["excluded_files_count"] += subdir_result["excluded_files_count"]
        
        # Обновляем human-readable размеры
        result["total_size_human"] = self._get_human_size(result["total_size_bytes"])
        result["included_size_human"] = self._get_human_size(result["included_size_bytes"])
        result["excluded_size_human"] = self._get_human_size(result["excluded_size_bytes"])
        
        return result
    
    def analyze_backup_size(self) -> Dict[str, Any]:
        """
        Анализирует размеры файлов, которые попадают в резервную копию
        
        Returns:
            Dict с полной информацией о размерах
        """
        print(f"Начинаем анализ размеров файлов в: {self.config.BITRIX_ROOT}")
        print(f"Паттерны исключений: {len(self.config.EXCLUDE_PATTERNS)} шт.")
        
        start_time = datetime.now()
        
        # Анализируем корневую директорию Bitrix
        analysis_result = self._analyze_directory(self.config.BITRIX_ROOT, self.config.BITRIX_ROOT)
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Создаем финальный отчет
        report = {
            "analysis_info": {
                "timestamp": start_time.strftime('%Y-%m-%d %H:%M:%S'),
                "execution_time_seconds": execution_time,
                "bitrix_root": self.config.BITRIX_ROOT,
                "exclude_patterns_count": len(self.config.EXCLUDE_PATTERNS),
                "exclude_patterns": self.config.EXCLUDE_PATTERNS
            },
            "summary": {
                "total_files": self.total_files,
                "total_size_bytes": self.total_size,
                "total_size_human": self._get_human_size(self.total_size),
                "included_files": self.total_files - self.excluded_files,
                "included_size_bytes": self.total_size - self.excluded_size,
                "included_size_human": self._get_human_size(self.total_size - self.excluded_size),
                "excluded_files": self.excluded_files,
                "excluded_size_bytes": self.excluded_size,
                "excluded_size_human": self._get_human_size(self.excluded_size),
                "exclusion_ratio_percent": round((self.excluded_size / self.total_size * 100) if self.total_size > 0 else 0, 2)
            },
            "directory_structure": analysis_result
        }
        
        return report
    
    def save_report(self, report: Dict[str, Any], output_file: str):
        """Сохраняет отчет в JSON файл"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"\n=== ОТЧЕТ ПО АНАЛИЗУ РАЗМЕРОВ ===")
        print(f"Всего файлов: {report['summary']['total_files']}")
        print(f"Общий размер: {report['summary']['total_size_human']}")
        print(f"Попадет в backup: {report['summary']['included_size_human']} ({report['summary']['included_files']} файлов)")
        print(f"Будет исключено: {report['summary']['excluded_size_human']} ({report['summary']['excluded_files']} файлов)")
        print(f"Процент исключений: {report['summary']['exclusion_ratio_percent']}%")
        print(f"Время анализа: {report['analysis_info']['execution_time_seconds']:.1f} сек")
        print(f"Отчет сохранен: {output_file}")


def main():
    """Основная функция"""
    analyzer = BackupSizeAnalyzer()
    
    # Генерируем имя выходного файла с временной меткой
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"/backup/logs/backup_size_analysis_{timestamp}.json"
    
    # Создаем директорию для логов если её нет
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    try:
        # Выполняем анализ
        report = analyzer.analyze_backup_size()
        
        # Сохраняем отчет
        analyzer.save_report(report, output_file)
        
    except Exception as e:
        print(f"Ошибка при анализе: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())