#!/usr/bin/env python3
"""
Анализатор отчета о размерах файлов backup'а
Извлекает ключевую информацию из детального JSON отчета
"""

import json
import sys
from typing import Dict, List, Tuple


def get_human_size(size_bytes: int) -> str:
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


def analyze_directory_sizes(directory_data: Dict, path: str = "") -> List[Tuple[str, int, int]]:
    """
    Рекурсивно анализирует размеры директорий
    
    Returns:
        List of tuples (path, included_size, total_size)
    """
    results = []
    
    # Добавляем текущую директорию
    if path:
        results.append((
            path,
            directory_data.get("included_size_bytes", 0),
            directory_data.get("total_size_bytes", 0)
        ))
    
    # Рекурсивно обрабатываем поддиректории
    subdirs = directory_data.get("subdirectories", {})
    for subdir_name, subdir_data in subdirs.items():
        subpath = f"{path}/{subdir_name}" if path else subdir_name
        results.extend(analyze_directory_sizes(subdir_data, subpath))
    
    return results


def find_largest_files(directory_data: Dict, path: str = "", top_n: int = 50) -> List[Tuple[str, str, int, bool]]:
    """
    Находит самые большие файлы
    
    Returns:
        List of tuples (file_path, file_name, size, included)
    """
    files = []
    
    # Обрабатываем файлы в текущей директории
    for file_info in directory_data.get("files", []):
        if file_info.get("type") == "file" and "size_bytes" in file_info:
            file_path = f"{path}/{file_info['name']}" if path else file_info['name']
            files.append((
                file_path,
                file_info['name'],
                file_info['size_bytes'],
                file_info.get('included', True)
            ))
    
    # Рекурсивно обрабатываем поддиректории
    subdirs = directory_data.get("subdirectories", {})
    for subdir_name, subdir_data in subdirs.items():
        subpath = f"{path}/{subdir_name}" if path else subdir_name
        files.extend(find_largest_files(subdir_data, subpath, top_n * 2))  # Берем больше для фильтрации
    
    # Сортируем по размеру и возвращаем топ
    files.sort(key=lambda x: x[2], reverse=True)
    return files[:top_n]


def analyze_exclusions(directory_data: Dict, path: str = "") -> Dict[str, List]:
    """
    Анализирует файлы по паттернам исключений
    """
    exclusions = {}
    
    # Обрабатываем файлы в текущей директории
    for file_info in directory_data.get("files", []):
        if not file_info.get('included', True) and file_info.get('excluded_by_pattern'):
            pattern = file_info['excluded_by_pattern']
            if pattern not in exclusions:
                exclusions[pattern] = []
            
            file_path = f"{path}/{file_info['name']}" if path else file_info['name']
            exclusions[pattern].append({
                'path': file_path,
                'size': file_info.get('size_bytes', 0)
            })
    
    # Рекурсивно обрабатываем поддиректории
    subdirs = directory_data.get("subdirectories", {})
    for subdir_name, subdir_data in subdirs.items():
        subpath = f"{path}/{subdir_name}" if path else subdir_name
        sub_exclusions = analyze_exclusions(subdir_data, subpath)
        
        for pattern, files in sub_exclusions.items():
            if pattern not in exclusions:
                exclusions[pattern] = []
            exclusions[pattern].extend(files)
    
    return exclusions


def main():
    if len(sys.argv) != 2:
        print("Использование: python3 analyze_backup_report.py <json_file>")
        return 1
    
    json_file = sys.argv[1]
    
    try:
        print(f"Загружаем отчет: {json_file}")
        with open(json_file, 'r', encoding='utf-8') as f:
            report = json.load(f)
        
        print(f"Отчет загружен успешно!")
        
        # Общая информация
        summary = report.get('summary', {})
        print(f"\n🔍 ОБЩАЯ СТАТИСТИКА:")
        print(f"   Общий размер: {summary.get('total_size_human', 'N/A')}")
        print(f"   Размер backup: {summary.get('included_size_human', 'N/A')}")
        print(f"   Исключено: {summary.get('excluded_size_human', 'N/A')} ({summary.get('exclusion_ratio_percent', 0)}%)")
        print(f"   Файлов в backup: {summary.get('included_files', 0)}")
        
        # Анализ директорий
        directory_structure = report.get('directory_structure', {})
        directory_sizes = analyze_directory_sizes(directory_structure)
        directory_sizes.sort(key=lambda x: x[1], reverse=True)  # Сортируем по included_size
        
        print(f"\n📁 ТОП-20 ДИРЕКТОРИЙ ПО РАЗМЕРУ (что попадает в backup):")
        for i, (path, included_size, total_size) in enumerate(directory_sizes[:20], 1):
            excluded_size = total_size - included_size
            print(f"{i:2d}. {path}")
            print(f"    В backup: {get_human_size(included_size)}")
            if excluded_size > 0:
                print(f"    Исключено: {get_human_size(excluded_size)}")
            print()
        
        # Анализ самых больших файлов
        largest_files = find_largest_files(directory_structure)
        
        print(f"\n📄 ТОП-15 САМЫХ БОЛЬШИХ ФАЙЛОВ:")
        for i, (file_path, file_name, size, included) in enumerate(largest_files[:15], 1):
            status = "✅ В backup" if included else "❌ Исключен"
            print(f"{i:2d}. {file_name} ({get_human_size(size)}) - {status}")
            print(f"    Путь: {file_path}")
            print()
        
        # Анализ исключений
        exclusions = analyze_exclusions(directory_structure)
        
        print(f"\n🚫 СТАТИСТИКА ИСКЛЮЧЕНИЙ ПО ПАТТЕРНАМ:")
        exclusion_stats = []
        for pattern, files in exclusions.items():
            total_size = sum(f['size'] for f in files)
            exclusion_stats.append((pattern, len(files), total_size))
        
        exclusion_stats.sort(key=lambda x: x[2], reverse=True)
        
        for pattern, files_count, total_size in exclusion_stats:
            print(f"   {pattern}: {files_count} файлов, {get_human_size(total_size)}")
        
        print(f"\n✅ Анализ завершен!")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())