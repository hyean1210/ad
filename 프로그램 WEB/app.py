import os
import io
import traceback
import datetime
import re
import json
import base64
import pandas as pd
import numpy as np
from flask import Flask, request, render_template, send_file, jsonify
from itertools import zip_longest
from collections import defaultdict
from openpyxl.styles import PatternFill


# --- Flask App Setup ---
app = Flask(__name__)
JOB_HISTORY_FILE = 'job_history.json'

# --- Helper Functions ---

def save_job_to_history(job_data):
    """Saves a job log to the JSON history file."""
    history = []
    if os.path.exists(JOB_HISTORY_FILE):
        with open(JOB_HISTORY_FILE, 'r', encoding='utf-8') as f:
            try:
                history = json.load(f)
            except json.JSONDecodeError:
                history = [] # File is empty or corrupt
    history.insert(0, job_data) # Add new job to the beginning
    with open(JOB_HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=4)


def get_excel_column_letters(num_columns):
    letters = []
    for i in range(num_columns):
        result = ""
        n = i
        while n >= 0:
            result = chr(ord('A') + n % 26) + result
            n = n // 26 - 1
            if n < 0: break
        letters.append(result)
    return letters

def excel_letter_to_index(letter):
    if not letter or not isinstance(letter, str): return 0
    index = 0
    try:
        for char in letter:
            index = index * 26 + (ord(char.upper()) - ord('A')) + 1
        return index - 1
    except TypeError:
        return 0
        
def col_to_excel(col_idx): # 0-indexed
    letter = ""
    col_idx_copy = col_idx
    while col_idx_copy >= 0:
        col_idx_copy, remainder = divmod(col_idx_copy, 26)
        letter = chr(ord('A') + remainder) + letter
        col_idx_copy -= 1
    return letter

def smart_read(file_obj, header_row=None, sheet_name=0):
    if not file_obj or not hasattr(file_obj, 'filename') or not file_obj.filename:
        raise ValueError("No file or filename provided.")

    filename = file_obj.filename
    _, file_extension = os.path.splitext(filename)
    read_opts = {'keep_default_na': False, 'na_values': ['']}
    
    file_obj.seek(0)
    file_content = io.BytesIO(file_obj.read())
    file_obj.seek(0)
    
    if file_extension.lower() == '.csv':
        try:
            return pd.read_csv(file_content, encoding='utf-8', engine='python', on_bad_lines='warn', header=header_row, **read_opts)
        except UnicodeDecodeError:
            file_content.seek(0)
            return pd.read_csv(file_content, encoding='cp949', engine='python', on_bad_lines='warn', header=header_row, **read_opts)
    elif file_extension.lower() in ['.xlsx', '.xls']:
        return pd.read_excel(file_content, header=header_row, sheet_name=sheet_name, **read_opts)
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")

def clean_country_name(name):
    try:
        name_str = str(name).split('(')[0].strip()
        return name_str.lower()
    except:
        return ""

def clean_value(val):
    if pd.isna(val): return np.nan
    s_val = str(val).strip()
    if s_val in ['...', '/..', '']: return np.nan
    if ',' in s_val:
        base, last_part = s_val.rsplit(',', 1)
        if '.' not in last_part and len(last_part) in [1, 2]: return np.nan
    s_val = s_val.replace(',', '')
    try:
        numeric_val = re.search(r'-?\d+\.?\d*', s_val)
        return float(numeric_val.group(0)) if numeric_val else np.nan
    except (ValueError, TypeError): return np.nan
    
def round_away_from_zero(x, decimals=0):
    if pd.isna(x): return np.nan
    if x == 0: return 0
    multiplier = 10 ** decimals
    return np.sign(x) * np.floor(np.abs(x) * multiplier + 0.5) / multiplier

def sum_with_nan(series):
    return series.sum(min_count=1)

def format_year_ranges(years):
    """연도 목록을 '2001~2005년', '2007년'과 같은 범위 문자열로 변환합니다."""
    if not years: return ""
    years = sorted(list(set(filter(pd.notna, years))))
    if not years: return ""
    ranges = []
    start_year = int(years[0])
    for i in range(1, len(years)):
        if int(years[i]) != int(years[i-1]) + 1:
            end_year = int(years[i-1])
            if start_year == end_year: ranges.append(f"{start_year}년")
            else: ranges.append(f"{start_year}~{end_year}년")
            start_year = int(years[i])
    end_year = int(years[-1])
    if start_year == end_year: ranges.append(f"{start_year}년")
    else: ranges.append(f"{start_year}~{end_year}년")
    return ", ".join(ranges)

# --- Full Comment Processing Logic (from KOREA_DEF_VER2.py) ---
def run_full_comment_processing(df_source_processed, files, source_cols_names, db_countries_set, temporary_rules={}):
    """
    주석 관련 시트를 생성하고, 의미상 충돌을 감지합니다. 웹 UI에서 받은 임시 규칙도 함께 사용합니다.
    """
    comment_rule_file = files['commentRuleFile']
    comment_rule_file.seek(0)
    
    # 1. Load rules from comment rule file
    try:
        mapping_sheet = smart_read(comment_rule_file, sheet_name="최종_주석_매핑", header_row=0)
    except Exception:
        raise ValueError("'주석 규칙 파일'에서 '최종_주석_매핑' 시트를 찾을 수 없습니다.")
    
    required_cols = ['원본 주석 내용', '주석 분류', '최종 주석 내용']
    if not all(col in mapping_sheet.columns for col in required_cols):
        raise ValueError(
            f"'주석 규칙 파일'의 '최종_주석_매핑' 시트에서 필수 열({', '.join(required_cols)})을 찾을 수 없습니다. "
            "파일의 첫 행에 헤더가 올바르게 있는지 확인해주세요."
        )

    try:
        comment_rule_file.seek(0)
        exception_sheet = smart_read(comment_rule_file, sheet_name="최종_주석처리_예외", header_row=0)
    except Exception:
        exception_sheet = pd.DataFrame() # 예외 시트는 선택사항

    mapping_dict = defaultdict(list)
    categories = []
    if not mapping_sheet.empty:
        for _, row in mapping_sheet.iterrows():
            keyword = str(row.get('원본 주석 내용', '')).strip().lower()
            category = str(row.get('주석 분류', ''))
            content = str(row.get('최종 주석 내용', ''))
            if keyword and category and content:
                mapping_dict[keyword].append((category, content))
        
        if '주석 분류' in mapping_sheet.columns:
            categories = mapping_sheet['주석 분류'].dropna().unique().tolist()

    # Add temporary rules from web UI
    for rule in temporary_rules.get('mapping_rules', []):
        keyword = rule['original'].strip().lower()
        category = rule['category']
        content = rule['final']
        if keyword and category and content:
            mapping_dict[keyword].append((category, content))
            if category not in categories:
                categories.append(category)

    exception_set = set()
    if not exception_sheet.empty and '예외 처리할 주석 내용' in exception_sheet.columns:
       exception_set.update({str(kw).strip().lower() for kw in exception_sheet['예외 처리할 주석 내용'].tolist() if pd.notna(kw) and str(kw).strip()})
    
    # 2. Process annotations row by row
    new_annotations = []
    all_mapped_notes = [] 
    
    df_with_comment_cols = df_source_processed.copy()
    for cat in categories:
        if cat not in df_with_comment_cols.columns:
            df_with_comment_cols[cat] = ''
    df_with_comment_cols['주석 의미 충돌'] = '' # 의미상 충돌 여부 열 추가
            
    def _classify(note_text):
        clean_text = str(note_text).strip().lower()
        if not clean_text: return [], []
        
        keywords = [k.strip() for k in clean_text.split('|')]
        
        mapped_data = []
        new_keywords = []

        for keyword in keywords:
            if not keyword: continue
            if keyword in exception_set: continue
            
            if keyword in mapping_dict:
                mapped_data.extend(mapping_dict[keyword])
            else:
                new_keywords.append(keyword)

        return mapped_data, new_keywords

    indicator_note_col = source_cols_names.get('indicator_note')
    source_note_col = source_cols_names.get('source_note')

    CONFLICT_PATTERNS = ["최소 연령", "최대 연령", "참조 기간", "연령"]

    for index, row in df_with_comment_cols.iterrows():
        notes_to_process_text = []
        if indicator_note_col and pd.notna(row.get(indicator_note_col)):
            notes_to_process_text.append(str(row[indicator_note_col]))
        if source_note_col and pd.notna(row.get(source_note_col)):
            notes_to_process_text.append(str(row[source_note_col]))
        
        full_note_text = " | ".join(notes_to_process_text)
        mapped_data, new_data = _classify(full_note_text)
        
        row_mapped_notes_for_edit_sheet = defaultdict(set)
        for category, final_note in mapped_data:
            row_mapped_notes_for_edit_sheet[category].add(final_note)
            
            note_info = {
                '국가명': row['표준 국가명'], '연도': row['시점'],
                '주석_카테고리': category, '최종_주석_내용': final_note
            }
            if '출처_그룹' in row:
                note_info['출처_그룹'] = row['출처_그룹']
            all_mapped_notes.append(note_info)
        
        found_patterns = defaultdict(int)
        all_notes_in_row = {note for note_set in row_mapped_notes_for_edit_sheet.values() for note in note_set}
        
        for note in all_notes_in_row:
            for pattern in CONFLICT_PATTERNS:
                if note.lower().strip().startswith(pattern.lower()):
                    found_patterns[pattern] += 1
        
        conflicts_found = [p for p, count in found_patterns.items() if count > 1]
        if conflicts_found:
            df_with_comment_cols.at[index, '주석 의미 충돌'] = f"충돌: {', '.join(conflicts_found)}"

        for category, notes in row_mapped_notes_for_edit_sheet.items():
            if category in df_with_comment_cols.columns:
                df_with_comment_cols.at[index, category] = '; '.join(sorted(list(notes)))

        if new_data:
             if row['표준 국가명'] in db_countries_set:
                 new_annotation_data = {
                    '국가명': row['표준 국가명'], '연도': row['시점'], '값': row.get(source_cols_names['value']),
                    '지표 주석열': row.get(indicator_note_col, ''), '출처 주석열': row.get(source_note_col, ''),
                    '매핑된 주석': '; '.join(sorted({note for _, note in mapped_data})),
                    '새로운 주석': '; '.join(sorted(list(set(new_data)))),
                }
                 new_annotations.append(new_annotation_data)
            
    output_sheets = {}
    if new_annotations:
        output_sheets['새로운 주석'] = pd.DataFrame(new_annotations)
    output_sheets['주석 편집 (선택된 출처)'] = df_with_comment_cols
    
    return output_sheets, all_mapped_notes


def generate_final_notes_sheet(all_mapped_notes, db_template_df):
    try:
        if not all_mapped_notes:
            return db_template_df.copy()

        df_all_notes = pd.DataFrame(all_mapped_notes)
        df_all_notes.drop_duplicates(inplace=True)

        translation_map = {
            'Ⅰ': '보고된 재해건수', 'Ⅱ': '보상된 재해건수', 'a': '보험가입자 십만명당',
            'b': '취업자 십만명당', 'c': '임금근로자 십만명당', 'd': '준상용근로자 십만명당',
            'HS': '가구조사', 'OE': '공식추계치', 'PC': '인구센서스',
            'LFS': '노동력조사', 'HIES': '가계조사', 'ADM': '취업사무소'
        }
        
        standard_keys_set = {'Ⅰ', 'Ⅱ', 'a', 'b', 'c', 'd'}

        def sort_standard_keys(keys):
            group1 = sorted([k for k in keys if k in ['Ⅰ', 'Ⅱ']])
            group2 = sorted([k for k in keys if k in ['a', 'b', 'c', 'd']])
            return group1 + group2

        standard_notes_map = {}
        final_notes_map = {}

        for country, country_group in df_all_notes.groupby('국가명'):
            if country_group.empty: continue

            notes_by_year = defaultdict(lambda: {'standard': set(), 'other': set()})
            for _, row in country_group.iterrows():
                note = row['최종_주석_내용']
                year = row['연도']
                if note in standard_keys_set:
                    notes_by_year[year]['standard'].add(note)
                else:
                    notes_by_year[year]['other'].add(note)

            if not notes_by_year:
                standard_notes_map[country] = ''
                final_notes_map[country] = ''
                continue
            
            max_year = max(notes_by_year.keys())
            latest_standard_notes = notes_by_year[max_year]['standard']
            
            sorted_latest_standard = sort_standard_keys(list(latest_standard_notes))
            country_standard_str = '/'.join(sorted_latest_standard)
            standard_notes_map[country] = country_standard_str

            all_exception_notes = [] 
            for year, notes in notes_by_year.items():
                year_standard_notes = notes['standard']
                sorted_year_standard = sort_standard_keys(list(year_standard_notes))
                year_standard_str = '/'.join(sorted_year_standard)
                
                if year_standard_str != country_standard_str and year_standard_str:
                    all_exception_notes.append((year, year_standard_str))

                for other_note in notes['other']:
                    all_exception_notes.append((year, other_note))
            
            if not all_exception_notes:
                final_notes_map[country] = ''
                continue
            
            notes_to_format = defaultdict(list)
            for year, note_text in all_exception_notes:
                notes_to_format[note_text].append(year)
                
            formatted_notes = []
            for note_text, years in notes_to_format.items():
                year_range_str = format_year_ranges(years)
                
                parts = note_text.split('/')
                translated_parts = [translation_map.get(p, p) for p in parts]
                translated_note = '/'.join(translated_parts)
                
                formatted_notes.append(f"{year_range_str} {translated_note}")
            
            final_notes_map[country] = '; '.join(sorted(formatted_notes))

        result_df = pd.DataFrame()
        db_col_a_name = db_template_df.columns[0]
        result_df[db_col_a_name] = db_template_df[db_col_a_name]
        
        matching_key = result_df[db_col_a_name].astype(str).str.strip()
        
        result_df['주석'] = matching_key.map(final_notes_map).fillna('')
        result_df.insert(2, '기준', matching_key.map(standard_notes_map).fillna(''))
        
        return result_df

    except Exception as e:
        print(f"'주석정리양식_결과' 생성 실패: {e}\n{traceback.format_exc()}")
        return db_template_df.copy()


# --- Full Data Processing Logic ---
def run_data_processing(files, params):
    try:
        source_cols = params['source_cols']
        map_cols = params['map_cols']
        rules = params.get('rules', {})
        temporary_rules = params.get('temporary_rules', {})
        source_criterion = params.get('sourceCriterion', '미 지정')
        manual_source_group = params.get('manualSource')
        apply_comments = params.get('applyComments', False)

        for d in [source_cols, map_cols]:
            for key, value in d.items():
                if not value or value == "없음": d[key] = None
        
        if not all(source_cols.get(k) for k in ['name', 'time', 'value']):
             raise ValueError("원본 데이터의 필수 열(국가명, 연도, 값)을 모두 지정해야 합니다.")
        if not all(map_cols.values()):
            raise ValueError("나라 정리 파일의 모든 열을 지정해야 합니다.")
        if apply_comments and not all(source_cols.get(k) for k in ['indicator_note', 'source_note']):
             raise ValueError("주석 처리 모드에서는 지표 주석과 출처 주석 열을 모두 지정해야 합니다.")

        start_year_str = params.get('startYear')
        end_year_str = params.get('endYear')
        start_year = int(start_year_str) if start_year_str and start_year_str.isdigit() else None
        end_year = int(end_year_str) if end_year_str and end_year_str.isdigit() else None
        if start_year and end_year and start_year > end_year: 
            raise ValueError("시작 연도가 종료 연도보다 클 수 없습니다.")

        selected_sources = rules.get('selected_sources', [])
        label_map = rules.get('label_map', {})
        label_order = rules.get('label_order', [])

        df_source_original = smart_read(files['sourceFile'], header_row=0)
        df_source = df_source_original.copy()
        df_country_map = smart_read(files['mappingFile'], header_row=0)
        df_template_input = smart_read(files['templateFile'], header_row=0)
        df_template_for_processing = smart_read(files['templateFile'], header_row=1)
        
        df_template_for_processing.columns = [str(c) for c in df_template_for_processing.columns]
        
        metadata_content = ""
        if 'metadataFile' in files and files['metadataFile']:
            try:
                files['metadataFile'].seek(0)
                metadata_content = files['metadataFile'].read().decode('utf-8')
            except Exception as e:
                print(f"Could not read metadata file: {e}")

        year_columns_from_template = [col for col in df_template_for_processing.columns if str(col).isnumeric()]
        for col in year_columns_from_template:
            df_template_for_processing[col] = df_template_for_processing[col].apply(clean_value)

        source_cols_names = {k: v for k,v in source_cols.items() if v}
        map_cols_names = {k: v for k,v in map_cols.items() if v}

        name_map_dict = df_country_map.dropna(subset=[map_cols_names['source_name']]).set_index(map_cols_names['source_name'])[map_cols_names['target_name']].to_dict()
        df_source['표준 국가명'] = df_source[source_cols_names['name']].map(name_map_dict).fillna(df_source[source_cols_names['name']])
        
        df_cleaned = df_source.copy()
        df_cleaned['시점'] = pd.to_numeric(df_cleaned[source_cols_names['time']], errors='coerce')
        df_cleaned[source_cols_names['value']] = df_cleaned[source_cols_names['value']].apply(clean_value)

        dropna_subset = ['시점', source_cols_names['value'], '표준 국가명']
        if source_cols_names.get('source_col'):
            dropna_subset.append(source_cols_names['source_col'])

        df_cleaned.dropna(subset=dropna_subset, inplace=True)
        df_cleaned['시점'] = df_cleaned['시점'].astype(int)

        if selected_sources and source_cols_names.get('source_col'):
            df_cleaned = df_cleaned[df_cleaned[source_cols_names['source_col']].isin(selected_sources)].copy()

        if source_cols_names.get('source_col'):
            df_cleaned['출처_그룹'] = df_cleaned[source_cols_names['source_col']].astype(str).str.split(' - ').str[0].str.strip()
        
        df_processed_for_main_logic = pd.DataFrame()
        selection_log_list = []

        if source_cols_names.get('source_col') and source_criterion != '미 지정':
            if source_criterion in ['대표 출처 기반 시계열 채우기', '단일 대표 출처만 사용'] and not manual_source_group:
                raise ValueError(f"'{source_criterion}' 기준을 선택했을 경우, 반드시 '대표 출처 수동 지정'에서 대표 출처를 선택해야 합니다.")

            best_sources_list = []
            
            country_primary_years_map = {}
            if source_criterion == '대표 출처 기반 시계열 채우기' and manual_source_group:
                primary_source_df = df_cleaned[df_cleaned['출처_그룹'] == manual_source_group]
                if not primary_source_df.empty:
                    country_primary_years_map = primary_source_df.groupby('표준 국가명')['시점'].apply(set).to_dict()

            best_source_groupby_cols = ['표준 국가명']
            if source_cols_names.get('label'):
                best_source_groupby_cols.append(source_cols_names.get('label'))

            for group_keys, group in df_cleaned.groupby(best_source_groupby_cols):
                if group.empty: continue
                
                log_entry_keys = ['국가명', '주요 구분'] if len(group_keys) > 1 else ['국가명']
                log_entry_values = group_keys if isinstance(group_keys, tuple) else (group_keys,)
                log_entry = dict(zip(log_entry_keys, log_entry_values))
                log_entry['선택기준'] = source_criterion

                if source_criterion == '대표 출처 기반 시계열 채우기':
                    country_name = group_keys[0]
                    primary_years = country_primary_years_map.get(country_name, set())
                    primary_df = group[group['출처_그룹'] == manual_source_group]
                    secondary_df = group[group['출처_그룹'] != manual_source_group]
                    
                    gap_filler_list = []
                    if not secondary_df.empty:
                        years_to_fill = secondary_df[~secondary_df['시점'].isin(primary_years)]['시점'].unique()
                        
                        for year in years_to_fill:
                            year_df = secondary_df[secondary_df['시점'] == year]
                            if year_df.empty: continue
                            
                            if year_df['출처_그룹'].nunique() > 1:
                                group_stats = year_df.groupby('출처_그룹').agg(최신연도=('시점', 'max'), 데이터개수=('시점', 'count')).reset_index()
                                winner_group_row = group_stats.sort_values(by=['최신연도', '데이터개수'], ascending=[False, False]).iloc[0]
                                best_source_for_year = winner_group_row['출처_그룹']
                                gap_filler_list.append(year_df[year_df['출처_그룹'] == best_source_for_year])
                            else:
                                gap_filler_list.append(year_df)

                    final_gap_filler_df = pd.concat(gap_filler_list, ignore_index=True) if gap_filler_list else pd.DataFrame()
                    final_group_df = pd.concat([primary_df, final_gap_filler_df], ignore_index=True)
                    best_sources_list.append(final_group_df)
                    log_entry['선택된 출처 그룹'] = f"대표: {manual_source_group} + 시계열 채우기"
                    selection_log_list.append(log_entry)

                elif source_criterion == '단일 대표 출처만 사용':
                    primary_df = group[group['출처_그룹'] == manual_source_group]
                    if not primary_df.empty:
                        best_sources_list.append(primary_df)
                        log_entry['선택된 출처 그룹'] = manual_source_group
                        selection_log_list.append(log_entry)
                    else: 
                        group_stats = group.groupby('출처_그룹').agg(최신연도=('시점', 'max'), 데이터개수=('시점', 'count')).reset_index()
                        if group_stats.empty: continue
                        winner_group_row = group_stats.sort_values(by=['최신연도', '데이터개수'], ascending=[False, False]).iloc[0]
                        winner_source_group = winner_group_row['출처_그룹']
                        best_sources_list.append(group[group['출처_그룹'] == winner_source_group])
                        log_entry['선택된 출처 그룹'] = f"{winner_source_group} (대표 출처 없음, 자동 선택)"
                        selection_log_list.append(log_entry)
                
                else: 
                    group_stats = group.groupby('출처_그룹').agg(최신연도=('시점', 'max'), 데이터개수=('시점', 'count')).reset_index()
                    if group_stats.empty: continue
                    winner_group_row = group_stats.sort_values(by=['최신연도', '데이터개수'], ascending=[False, False]).iloc[0]
                    best_sources_list.append(group[group['출처_그룹'] == winner_group_row['출처_그룹']])
                    log_entry.update({'선택된 출처 그룹': winner_group_row['출처_그룹']})
                    selection_log_list.append(log_entry)


            if best_sources_list:
                df_processed_for_main_logic = pd.concat(best_sources_list, ignore_index=True)
            else:
                df_processed_for_main_logic = pd.DataFrame(columns=df_cleaned.columns)
        else: 
            df_processed_for_main_logic = df_cleaned

        df_selection_log = pd.DataFrame(selection_log_list)

        if start_year and end_year:
            df_processed_for_main_logic = df_processed_for_main_logic[(df_processed_for_main_logic['시점'] >= start_year) & (df_processed_for_main_logic['시점'] <= end_year)]
            
        if df_processed_for_main_logic.empty:
            raise ValueError("처리할 데이터가 없습니다. (파일, 열, 필터링 조건 확인)")

        template_country_col_name = df_template_for_processing.columns[0]
        df_template_for_processing[template_country_col_name] = df_template_for_processing[template_country_col_name].astype(str).str.strip()
        template_countries_set = set(df_template_for_processing[template_country_col_name].unique())
        
        comment_sheets = {}
        all_mapped_notes = [] 
        if apply_comments and 'commentRuleFile' in files:
            comment_sheets, all_mapped_notes = run_full_comment_processing(df_processed_for_main_logic, files, source_cols_names, template_countries_set, temporary_rules)

        main_sheets = {}
        df_for_pivot = df_processed_for_main_logic.copy()

        pivot_index_cols = ['표준 국가명']
        if source_cols_names.get('label'):
            pivot_index_cols.append(source_cols_names.get('label'))
        if source_cols_names.get('detail_label'):
            pivot_index_cols.append(source_cols_names.get('detail_label'))
        if source_cols_names.get('sub_detail_label'):
            pivot_index_cols.append(source_cols_names.get('sub_detail_label'))


        if source_cols_names.get('label') and label_map:
            df_for_pivot[source_cols_names['label']] = df_for_pivot[source_cols_names['label']].astype(str).replace(label_map)
        if source_cols_names.get('label') and label_order:
            df_for_pivot[source_cols_names['label']] = pd.Categorical(df_for_pivot[source_cols_names['label']], categories=label_order, ordered=True)
        
        df_pivot = df_for_pivot.pivot_table(index=pivot_index_cols, columns='시점', values=source_cols_names['value'], aggfunc=sum_with_nan)
        df_pivot.columns = df_pivot.columns.astype(str)
        
        processed_countries_set = set(df_processed_for_main_logic['표준 국가명'].unique())
        extra_countries_set = processed_countries_set - template_countries_set
        
        df_extra_pivot = pd.DataFrame()
        if extra_countries_set:
            df_extra = df_processed_for_main_logic[df_processed_for_main_logic['표준 국가명'].isin(extra_countries_set)]
            if all(col in df_extra.columns for col in pivot_index_cols):
                 df_extra_pivot = df_extra.pivot_table(index=pivot_index_cols, columns='시점', values=source_cols_names['value'], aggfunc=sum_with_nan, observed=False).reset_index()

        final_index = [df_template_for_processing.columns[0]]
        pivot_rename_map = {'표준 국가명': final_index[0]}
        if source_cols_names.get('label') and len(df_template_for_processing.columns) > 1 and not str(df_template_for_processing.columns[1]).isnumeric():
            final_index.append(df_template_for_processing.columns[1])
            pivot_rename_map[source_cols_names.get('label')] = df_template_for_processing.columns[1]
        if source_cols_names.get('detail_label') and len(df_template_for_processing.columns) > 2 and not str(df_template_for_processing.columns[2]).isnumeric():
            final_index.append(df_template_for_processing.columns[2])
            pivot_rename_map[source_cols_names.get('detail_label')] = df_template_for_processing.columns[2]
        if source_cols_names.get('sub_detail_label') and len(df_template_for_processing.columns) > 3 and not str(df_template_for_processing.columns[3]).isnumeric():
            final_index.append(df_template_for_processing.columns[3])
            pivot_rename_map[source_cols_names.get('sub_detail_label')] = df_template_for_processing.columns[3]

        
        df_pivot.reset_index(inplace=True)
        df_pivot.rename(columns=pivot_rename_map, inplace=True)
        
        all_year_columns = [str(y) for y in year_columns_from_template] 

        # ##########################################################################
        # ##               '원본정리' 생성 로직: update -> merge로 변경             ##
        # ##########################################################################
        
        # 1. '주요 구분'이 하나뿐이고 DB 양식에 항목 구분이 없는 특수 케이스 처리
        is_single_item_mode = len(label_order) == 1
        db_has_no_real_labels = True
        if len(df_template_for_processing.columns) > 1:
            # is_numeric_dtype으로 해당 열이 숫자인지 확인
            if not pd.api.types.is_numeric_dtype(df_template_for_processing.columns[1]):
                # .any()를 사용하여 열에 하나라도 유효한(비어있지 않은) 값이 있는지 확인
                if df_template_for_processing.iloc[:, 1].dropna().any():
                    db_has_no_real_labels = False

        if is_single_item_mode and db_has_no_real_labels:
            if len(df_template_for_processing.columns) > 1:
                db_label_col_name = df_template_for_processing.columns[1]
                # 피벗 테이블과 최종 인덱스 모두에서 항목 열을 제거
                if db_label_col_name in df_pivot.columns:
                    df_pivot.drop(columns=[db_label_col_name], inplace=True)
                if db_label_col_name in final_index:
                    final_index.remove(db_label_col_name)

        # 2. DB 양식에서 구조(인덱스 열)만 가져와서 최종 결과의 틀로 사용
        #    .drop_duplicates()로 고유한 행 조합만 남겨서 안정성 확보
        df_template_structure = df_template_for_processing[final_index].copy().drop_duplicates()

        # 3. Merge를 위한 키(인덱스) 데이터 타입 및 공백 통일
        for col in final_index:
            df_template_structure[col] = df_template_structure[col].astype(str).str.strip()
            if col in df_pivot.columns:
                df_pivot[col] = df_pivot[col].astype(str).str.strip()

        # 4. DB 구조와 가공된 데이터를 'left merge'로 안전하게 병합
        df_final = pd.merge(df_template_structure, df_pivot, on=final_index, how='left')
        
        # 5. DB 양식에 정의된 열만 남도록 필터링 (원본의 불필요한 연도 열 제거)
        final_columns_to_keep = final_index + all_year_columns
        # merge 과정에서 생겼을 수 있는 불필요한 열을 제거하기 위해, df_final에 실제로 존재하는 열만 선택
        final_columns_to_keep = [col for col in final_columns_to_keep if col in df_final.columns]
        df_final = df_final[final_columns_to_keep]
        
        # 최종 값 정리 (반올림 등)
        year_cols_in_final = [col for col in df_final.columns if str(col).isnumeric()]
        for col in year_cols_in_final:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').apply(lambda x: round_away_from_zero(x, 5))
        
        main_sheets['원본정리'] = df_final
        if not df_extra_pivot.empty:
            main_sheets['DB에 없는 나라'] = df_extra_pivot

        df_db_original = df_template_for_processing.copy()
        for year in all_year_columns:
            if year not in df_db_original.columns:
                df_db_original[year] = np.nan

        df_db_original_indexed = df_db_original.set_index(final_index)
        df_final_indexed = df_final.set_index(final_index)
        df_db_aligned, df_final_aligned = df_db_original_indexed.align(df_final_indexed, join='outer', axis=0)
        
        mask_both_nan = df_final_aligned[all_year_columns].isna() & df_db_aligned[all_year_columns].isna()
        df_check_values = df_final_aligned[all_year_columns].fillna(0).subtract(df_db_aligned[all_year_columns].fillna(0))
        df_check_values[mask_both_nan] = np.nan
        df_check = df_check_values.reset_index()

        df_remarks = pd.DataFrame()
        check_melted = df_check.melt(id_vars=final_index, var_name='연도', value_name='차이값')
        check_melted.dropna(subset=['차이값'], inplace=True)
        check_melted = check_melted[check_melted['차이값'] != 0].copy()

        if not check_melted.empty:
            final_melted = df_final.melt(id_vars=final_index, var_name='연도', value_name='원본정리_값')
            merge_cols = final_index + ['연도']
            for col in merge_cols:
                check_melted[col] = check_melted[col].astype(str)
                final_melted[col] = final_melted[col].astype(str)
            remarks_merged = pd.merge(check_melted, final_melted, on=merge_cols, how='left')
            remarks_merged['원본정리_값'].fillna(0, inplace=True)
            condition = (
                (remarks_merged['원본정리_값'] == 0) & (remarks_merged['차이값'] != 0) | 
                (remarks_merged['원본정리_값'] != 0) & (abs(remarks_merged['차이값']) > abs(remarks_merged['원본정리_값'] * 0.2))
            )
            df_remarks = remarks_merged[condition].copy()
            if not df_remarks.empty:
                df_remarks.sort_values(by='차이값', key=abs, ascending=False, inplace=True)
        
        main_sheets['점검_데이터'] = {
            'df_final': df_final,
            'df_db_original_for_check': df_db_original, 
            'df_check': df_check,
            'final_index': final_index,
            'all_year_columns': all_year_columns
        }
        if not df_remarks.empty:
            main_sheets['비고'] = df_remarks

        summary = {
            'processed_rows': len(df_processed_for_main_logic),
            'extra_countries_count': len(extra_countries_set),
            'discrepancies_found': len(df_remarks) if not df_remarks.empty else 0,
            'db_countries_count': len(template_countries_set),
            'source_countries_count': len(processed_countries_set)
        }

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            header_format = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1, 'align': 'center'})
            orange_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
            gray_format = workbook.add_format({'bg_color': '#F2F2F2'})
            metadata_format = workbook.add_format({'text_wrap': True, 'valign': 'top'})

            today_str = datetime.datetime.now().strftime("%m.%d")
            db_sheet_name = f"DB {today_str}"
            source_sheet_name = f"원본 {today_str}"

            all_sheets = {
                db_sheet_name: df_template_input,
                source_sheet_name: df_source_original,
                '나라': df_country_map,
            }
            if 'DB에 없는 나라' in main_sheets:
                all_sheets['DB에 없는 나라'] = main_sheets['DB에 없는 나라']
            
            all_sheets['원본정리'] = main_sheets['원본정리']
            
            if not df_selection_log.empty:
                all_sheets['출처 선택 기준'] = df_selection_log
            
            if '비고' in main_sheets:
                all_sheets['비고'] = main_sheets['비고']
            
            if apply_comments:
                if comment_sheets.get('주석 편집 (선택된 출처)') is not None:
                     all_sheets['주석 편집 (선택된 출처)'] = comment_sheets['주석 편집 (선택된 출처)']
                if comment_sheets.get('새로운 주석') is not None:
                     all_sheets['새로운 주석'] = comment_sheets['새로운 주석']
                
                if all_mapped_notes:
                    df_final_notes = generate_final_notes_sheet(all_mapped_notes, df_template_for_processing)
                    all_sheets['주석정리양식_결과'] = df_final_notes

            sheet_order = [
                db_sheet_name, source_sheet_name, '나라', 'DB에 없는 나라', '원본정리', '점검', 
                '주석 편집 (선택된 출처)', '새로운 주석', '주석정리양식_결과',
            ]

            for sheet_name in sheet_order:
                if sheet_name == '점검':
                    check_data = main_sheets.get('점검_데이터')
                    if not check_data: continue

                    check_sheet_name = f"점검 {today_str}"
                    worksheet_check = workbook.add_worksheet(check_sheet_name)
                    start_col = 0
                    
                    df_final_check = check_data['df_final']
                    df_db_check = check_data['df_db_original_for_check']
                    df_diff_check = check_data['df_check']
                    final_index_check = check_data['final_index']
                    all_year_columns_check = check_data['all_year_columns']

                    worksheet_check.write(0, start_col, "가공 결과 (원본정리)", header_format)
                    df_final_check.to_excel(writer, sheet_name=check_sheet_name, startrow=1, startcol=start_col, index=False)
                    start_col += len(df_final_check.columns) + 2

                    db_cols_to_keep = final_index_check + all_year_columns_check
                    df_db_original_for_check = df_db_check[[col for col in db_cols_to_keep if col in df_db_check.columns]]
                    worksheet_check.write(0, start_col, "원본 DB 값", header_format)
                    df_db_original_for_check.to_excel(writer, sheet_name=check_sheet_name, startrow=1, startcol=start_col, index=False)
                    start_col += len(df_db_original_for_check.columns) + 2

                    check_start_col = start_col
                    worksheet_check.write(0, check_start_col, "점검 (차이)", header_format)
                    df_diff_check.to_excel(writer, sheet_name=check_sheet_name, startrow=1, startcol=check_start_col, index=False)

                    if not df_diff_check.empty:
                        first_row, last_row = 2, 2 + len(df_diff_check) - 1
                        first_col = check_start_col + len(final_index_check)
                        last_col = check_start_col + len(df_diff_check.columns) - 1
                        worksheet_check.conditional_format(first_row, first_col, last_row, last_col,
                                                           {'type': 'cell', 'criteria': '!=', 'value': 0, 'format': orange_format})
                        worksheet_check.conditional_format(first_row, first_col, last_row, last_col,
                                                           {'type': 'blanks', 'format': gray_format})
                
                elif sheet_name == db_sheet_name:
                    worksheet_db = workbook.add_worksheet(sheet_name)
                    df_to_write = all_sheets[sheet_name]
                    
                    if metadata_content:
                        worksheet_db.set_column('A:A', 50)
                        row_height = 15 * (metadata_content.count('\n') + 1)
                        worksheet_db.set_row(0, row_height)
                        worksheet_db.write('A1', metadata_content, metadata_format)
                    
                    for c_idx, col_name in enumerate(df_to_write.columns):
                        worksheet_db.write(0, 10 + c_idx, col_name, header_format)
                    
                    for r_idx, row in df_to_write.iterrows():
                        for c_idx, value in enumerate(row.values):
                            if pd.notna(value): 
                                worksheet_db.write(r_idx + 1, 10 + c_idx, value)

                elif sheet_name in all_sheets:
                    df_to_write = all_sheets.get(sheet_name)
                    if df_to_write is not None and not df_to_write.empty:
                        df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)
            

        output.seek(0)
        return output, summary
    except Exception as e:
        traceback.print_exc()
        raise e

# --- Page Routes ---
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/processing')
def processing_page():
    with_comments = request.args.get('with_comments', 'false').lower() == 'true'
    return render_template('processing.html', with_comments=with_comments)

@app.route('/country-cleanup')
def country_cleanup_page():
    return render_template('country_cleanup.html')

@app.route('/processing-choice')
def processing_choice_page():
    return render_template('processing_choice.html')

@app.route('/comment-rule-maker')
def comment_rule_maker_page():
    return render_template('comment_rule_maker.html')

@app.route('/history')
def history_page():
    return render_template('history.html')


# --- API Routes ---

@app.route('/get_history', methods=['GET'])
def get_history_route():
    if not os.path.exists(JOB_HISTORY_FILE):
        return jsonify([])
    try:
        with open(JOB_HISTORY_FILE, 'r', encoding='utf-8') as f:
            history = json.load(f)
        return jsonify(history)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_columns', methods=['POST'])
def get_columns_route():
    if 'file' not in request.files: return jsonify({'error': '파일이 없습니다.'}), 400
    file = request.files['file']
    if not file or not file.filename: return jsonify({'error': '파일이 선택되지 않았습니다.'}), 400
    
    try:
        df_header = smart_read(file, header_row=0)
        return jsonify({'columns': df_header.columns.tolist()})
    except Exception as e:
        try:
            df_header = smart_read(file, header_row=1)
            return jsonify({'columns': df_header.columns.tolist()})
        except Exception as e2:
             return jsonify({'error': f'파일 열 분석 중 오류: {str(e2)}'}), 500

@app.route('/get_source_hierarchy', methods=['POST'])
def get_source_hierarchy_route():
    if 'file' not in request.files or 'column' not in request.form:
        return jsonify({'error': '파일과 열 이름을 모두 제공해야 합니다.'}), 400
    file = request.files['file']
    column_name = request.form.get('column')
    try:
        df = smart_read(file, header_row=0)
        if column_name not in df.columns:
             return jsonify({'error': f"'{column_name}' 열을 찾을 수 없습니다."}), 400
        
        unique_sources = sorted(df[column_name].dropna().unique())
        hierarchy = {}
        for source in unique_sources:
            group = str(source).split(' - ')[0].strip()
            if group not in hierarchy:
                hierarchy[group] = []
            hierarchy[group].append(source)
        return jsonify({'hierarchy': hierarchy})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_unique_values', methods=['POST'])
def get_unique_values_route():
    if 'file' not in request.files: return jsonify({'error': '파일이 없습니다.'}), 400
    file = request.files['file']
    column_name = request.form.get('column')
    column_index_str = request.form.get('columnIndex')
    try:
        header = 1 if column_index_str else 0
        df = smart_read(file, header_row=header)
        unique_values = []
        if column_name and column_name in df.columns:
            unique_values = sorted(df[column_name].dropna().astype(str).unique().tolist())
        elif column_index_str:
            col_index = int(column_index_str)
            if col_index < len(df.columns):
                unique_values = sorted(df.iloc[:, col_index].dropna().astype(str).unique().tolist())
            else:
                return jsonify({'error': '열 인덱스가 범위를 벗어났습니다.'}), 400

        return jsonify({'values': unique_values})
    except Exception as e:
        return jsonify({'error': f'파일 처리 중 오류: {str(e)}'}), 500

@app.route('/get_source_candidates', methods=['POST'])
def get_source_candidates_route():
    if 'file' not in request.files or 'source_col' not in request.form or 'time_col' not in request.form:
        return jsonify({'error': '파일, 출처 열, 연도 열을 모두 제공해야 합니다.'}), 400
    file = request.files['file']
    source_col_name = request.form.get('source_col')
    time_col_name = request.form.get('time_col')
    try:
        df = smart_read(file, header_row=0)
        if source_col_name not in df.columns or time_col_name not in df.columns:
            return jsonify({'error': '선택한 열을 파일에서 찾을 수 없습니다.'}), 400

        df['출처_그룹'] = df[source_col_name].astype(str).str.split(' - ').str[0].str.strip()
        df['시점'] = pd.to_numeric(df[time_col_name], errors='coerce')
        stats = df.groupby('출처_그룹').agg(최신연도=('시점', 'max'), 데이터개수=('시점', 'count')).reset_index()
        sorted_stats = stats.sort_values(by=['최신연도', '데이터개수'], ascending=[False, False])
        candidates = sorted_stats['출처_그룹'].tolist()
        return jsonify({'candidates': candidates})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def run_country_matching(source_file, db_file, standard_file, cols):
    try:
        df_source = smart_read(source_file, header_row=0)
        df_db = smart_read(db_file, header_row=0)
        
        source_country_col = cols['source']
        db_country_col = cols['db']

        master_map = {}
        xls = pd.ExcelFile(standard_file)
        all_sheets = xls.sheet_names
        
        base_sheet_name = '업무용DB국가코드'
        if base_sheet_name not in all_sheets:
            base_sheet_name = all_sheets[0]

        temp_df = pd.read_excel(xls, sheet_name=base_sheet_name, header=None)
        header_row_index = 0
        for i, row in temp_df.iterrows():
            if '한글명' in str(row.values) and '영문명' in str(row.values):
                header_row_index = i
                break
        
        base_df = pd.read_excel(xls, sheet_name=base_sheet_name, header=header_row_index)
        
        eng_col_name = None
        for col in ['영문명', 'DB 영문명', '영문 국가명']:
            if col in base_df.columns:
                eng_col_name = col
                break
        if not eng_col_name:
            raise ValueError(f"기준 시트({base_sheet_name})에서 '영문명' 열을 찾을 수 없습니다.")

        base_df['표준영문명'] = base_df[eng_col_name].str.strip()
        
        for _, row in base_df.iterrows():
            if pd.notna(row['표준영문명']):
                std_eng_name = row['표준영문명']
                master_map[clean_country_name(std_eng_name)] = std_eng_name
                if '한글명' in base_df.columns and pd.notna(row['한글명']):
                    master_map[clean_country_name(row['한글명'])] = std_eng_name

        for sheet in all_sheets:
            if sheet == base_sheet_name:
                continue
            try:
                df_org = pd.read_excel(xls, sheet_name=sheet, header=None)
                start_row = 0
                for i, row in df_org.iterrows():
                    if any(k in str(row.values) for k in ['DB', '한글명', '영문명', '국가']):
                        start_row = i
                        break
                
                df_org = pd.read_excel(xls, sheet_name=sheet, header=start_row)
                
                if len(df_org.columns) < 3:
                    continue

                db_eng_col = df_org.columns[1]
                org_col = df_org.columns[2]

                for _, row in df_org.iterrows():
                    db_name = row.get(db_eng_col)
                    org_name = row.get(org_col)

                    if pd.notna(db_name) and pd.notna(org_name):
                        std_name_series = base_df[base_df[eng_col_name].str.strip() == str(db_name).strip()]
                        if not std_name_series.empty:
                            std_name = std_name_series.iloc[0]['표준영문명']
                            master_map[clean_country_name(org_name)] = std_name
            except Exception:
                pass
        
        df_source['표준국가명'] = df_source[source_country_col].apply(
            lambda x: master_map.get(clean_country_name(x), 'N/A')
        )
        df_db['표준국가명'] = df_db[db_country_col].apply(
            lambda x: master_map.get(clean_country_name(x), 'N/A')
        )
        
        source_map = df_source[['표준국가명', source_country_col]].rename(columns={source_country_col: '원본_국가명'}).drop_duplicates()
        db_map = df_db[['표준국가명', db_country_col]].rename(columns={db_country_col: 'DB_국가명'}).drop_duplicates()
        
        summary_df = pd.merge(source_map, db_map, on='표준국가명', how='inner')
        summary_df = summary_df[summary_df['표준국가명'] != 'N/A']
        summary_df = summary_df[['원본_국가명', 'DB_국가명', '표준국가명']]
        summary_df.rename(columns={'표준국가명': '매칭된_표준국가명'}, inplace=True)
        
        unmatched_source_list = df_source[df_source['표준국가명'] == 'N/A'][source_country_col].unique().tolist()
        unmatched_db_list = df_db[df_db['표준국가명'] == 'N/A'][db_country_col].unique().tolist()

        failure_summary_df = pd.DataFrame(
            list(zip_longest(unmatched_source_list, unmatched_db_list)),
            columns=['원본_매칭실패_국가명', 'DB_매칭실패_국가명']
        )

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            summary_df.to_excel(writer, sheet_name='매칭_요약', index=False)
            df_source.to_excel(writer, sheet_name='원본파일_매칭결과포함', index=False)
            if not failure_summary_df.empty:
                failure_summary_df.to_excel(writer, sheet_name='매칭_실패_요약', index=False)

        output.seek(0)
        return output
    except Exception as e:
        raise e

@app.route('/run_country_matching', methods=['POST'])
def run_country_matching_route():
    try:
        if not all(f in request.files for f in ['sourceFile', 'dbFile', 'masterFile']):
            return jsonify({'error': '세 개의 파일을 모두 업로드해야 합니다.'}), 400
        files = { 'source_file': request.files['sourceFile'], 'db_file': request.files['dbFile'], 'standard_file': request.files['masterFile'] }
        cols = { 'source': request.form.get('sourceCol'), 'db': request.form.get('dbCol') }
        if not all(cols.values()):
            return jsonify({'error': '원본 데이터와 DB 파일의 열을 선택해야 합니다.'}), 400
        
        output_filename = request.form.get('outputFileName', '자동매칭_나라정리_결과.xlsx')
        if not output_filename.lower().endswith(('.xlsx', '.xls')):
            output_filename += '.xlsx'
            
        result_io = run_country_matching(files['source_file'], files['db_file'], files['standard_file'], cols)
        return send_file(result_io, download_name=output_filename, as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f"매칭 처리 중 오류 발생: {str(e)}"}), 500
        
@app.route('/run_comment_rule_making', methods=['POST'])
def run_comment_rule_making_route():
    try:
        final_rules = json.loads(request.form.get('final_rules'))
        
        mapping_rules = final_rules.get('mapping_rules', [])
        exception_rules = final_rules.get('exception_rules', [])

        df_mapping_final = pd.DataFrame(mapping_rules)
        if not df_mapping_final.empty:
            df_mapping_final = df_mapping_final[['original', 'category', 'final']]
            df_mapping_final.columns = ['원본 주석 내용', '주석 분류', '최종 주석 내용']
            df_mapping_final = df_mapping_final.drop_duplicates().reset_index(drop=True)
        
        df_exception_final = pd.DataFrame(exception_rules)
        if not df_exception_final.empty:
             df_exception_final.columns = ['예외 처리할 주석 내용']
             df_exception_final = df_exception_final.drop_duplicates().reset_index(drop=True)


        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_mapping_final.to_excel(writer, sheet_name='최종_주석_매핑', index=False)
            df_exception_final.to_excel(writer, sheet_name='최종_주석처리_예외', index=False)

        output.seek(0)
        
        output_filename = request.form.get('outputFileName', '주석_규칙_파일.xlsx')
        if not output_filename.lower().endswith(('.xlsx', '.xls')):
            output_filename += '.xlsx'

        return send_file(
            output,
            download_name=output_filename,
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f"주석 규칙 생성 중 오류: {str(e)}"}), 500

@app.route('/analyze_master_comments', methods=['POST'])
def analyze_master_comments_route():
    try:
        if 'file' not in request.files:
             return jsonify({'error': '파일이 없습니다.'}), 400
        
        file = request.files['file']
        note_cols_str = request.form.get('noteCols')
        note_cols = json.loads(note_cols_str) if note_cols_str else []
        category_cols_str = request.form.get('categoryCols')
        category_cols = json.loads(category_cols_str) if category_cols_str else []

        if not note_cols or not category_cols:
            return jsonify({'error': '하나 이상의 주석내용 열과 카테고리 열을 선택해야 합니다.'}), 400

        df = smart_read(file, header_row=0)
        
        keyword_contexts = defaultdict(list)
        ambiguous_groups = []
        ambiguous_row_keywords = set()
        group_id_counter = 0

        for index, row in df.iterrows():
            note_content_parts = [str(row.get(nc, '')) for nc in note_cols if pd.notna(row.get(nc))]
            note_content = ' | '.join(note_content_parts)
            row_keywords = sorted(list({k.strip() for k in note_content.split('|') if k.strip()}))
            
            row_classifications = {}
            for cat_col in category_cols:
                if pd.notna(row.get(cat_col)) and str(row.get(cat_col)).strip():
                    row_classifications[cat_col] = str(row[cat_col])

            if len(row_keywords) > 1 and row_classifications:
                ambiguous_groups.append({
                    "type": "ambiguous_group",
                    "id": f"group_{group_id_counter}",
                    "keywords": row_keywords,
                    "categories": row_classifications
                })
                ambiguous_row_keywords.update(row_keywords)
                group_id_counter += 1
            else:
                for keyword in row_keywords:
                    keyword_contexts[keyword].append(row_classifications)
        
        auto_resolved_rules = []
        auto_exception_keywords = set()
        needs_review_keywords = set()
        
        keywords_to_analyze_automatically = set(keyword_contexts.keys()) - ambiguous_row_keywords

        for keyword in keywords_to_analyze_automatically:
            contexts = keyword_contexts[keyword]
            has_content = any(contexts)
            if not has_content:
                auto_exception_keywords.add(keyword)
                continue
            all_classifications_for_keyword = defaultdict(set)
            for context in contexts:
                for cat, val in context.items():
                    all_classifications_for_keyword[cat].add(val)
            is_simple_rule = (len(all_classifications_for_keyword) == 1 and len(list(all_classifications_for_keyword.values())[0]) == 1)
            if is_simple_rule:
                category_name = list(all_classifications_for_keyword.keys())[0]
                final_value = list(all_classifications_for_keyword[category_name])[0]
                if final_value == '*':
                    final_value = keyword
                auto_resolved_rules.append({'original': keyword, 'category': category_name, 'final': final_value})
            else:
                needs_review_keywords.add(keyword)
        
        simple_review_items = [{"type": "simple_keyword", "keyword": kw} for kw in sorted(list(needs_review_keywords))]
        
        return jsonify({
            'needs_review': simple_review_items + ambiguous_groups,
            'auto_resolved': auto_resolved_rules,
            'auto_exceptions': sorted(list(auto_exception_keywords)),
            'category_columns': category_cols
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f"분석 중 오류: {str(e)}"}), 500


@app.route('/process', methods=['POST'])
def process_files_route():
    try:
        if not all(f in request.files for f in ['sourceFile', 'mappingFile', 'templateFile']):
            return jsonify({'error': '필수 파일(원본, 나라 정리, DB 양식)이 누락되었습니다.'}), 400
        
        files_dict = {
            'sourceFile': request.files['sourceFile'],
            'mappingFile': request.files['mappingFile'],
            'templateFile': request.files['templateFile']
        }
        if 'commentRuleFile' in request.files and request.files['commentRuleFile'].filename != '':
            files_dict['commentRuleFile'] = request.files['commentRuleFile']
        
        if 'metadataFile' in request.files and request.files['metadataFile'].filename != '':
            files_dict['metadataFile'] = request.files['metadataFile']

        params = {
            'source_cols': json.loads(request.form.get('sourceCols')),
            'map_cols': json.loads(request.form.get('mapCols')),
            'rules': json.loads(request.form.get('rules', '{}')),
            'temporary_rules': json.loads(request.form.get('temporary_rules', '{}')),
            'sourceCriterion': request.form.get('sourceCriterion'),
            'manualSource': request.form.get('manualSource'),
            'startYear': request.form.get('startYear'),
            'endYear': request.form.get('endYear'),
            'applyComments': request.form.get('applyComments') == 'true'
        }
        
        output_filename = request.form.get('outputFileName', '최종_결과물.xlsx')
        if not output_filename.lower().endswith(('.xlsx', '.xls')):
            output_filename += '.xlsx'

        result_excel_io, summary = run_data_processing(files_dict, params)

        serializable_params = json.loads(json.dumps(params, default=str))
        job_log = {
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'output_filename': output_filename,
            'settings': serializable_params,
            'summary': summary
        }
        save_job_to_history(job_log)

        result_excel_io.seek(0)
        file_data_base64 = base64.b64encode(result_excel_io.read()).decode('utf-8')

        return jsonify({
            'summary': summary,
            'file_data': file_data_base64,
            'filename': output_filename
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f"처리 중 오류 발생: {str(e)}"}), 500

@app.route('/find_new_comments', methods=['POST'])
def find_new_comments_route():
    try:
        if not all(f in request.files for f in ['sourceFile', 'commentRuleFile']):
            return jsonify({'error': '원본 파일과 주석 규칙 파일이 모두 필요합니다.'}), 400

        files = {
            'sourceFile': request.files['sourceFile'],
            'commentRuleFile': request.files['commentRuleFile']
        }
        params = {
            'source_cols': json.loads(request.form.get('sourceCols'))
        }

        df_source = smart_read(files['sourceFile'], header_row=0)
        
        comment_rule_file = files['commentRuleFile']
        comment_rule_file.seek(0)
        mapping_sheet = smart_read(comment_rule_file, sheet_name="최종_주석_매핑", header_row=0)
        
        comment_rule_file.seek(0)
        try:
            exception_sheet = smart_read(comment_rule_file, sheet_name="최종_주석처리_예외", header_row=0)
            exception_set = {str(kw).strip().lower() for kw in exception_sheet.iloc[:, 0].tolist() if pd.notna(kw)}
        except:
            exception_set = set()

        existing_keywords = {str(kw).strip().lower() for kw in mapping_sheet.iloc[:, 0].tolist() if pd.notna(kw)}
        all_known_keywords = existing_keywords.union(exception_set)
        
        categories = sorted(mapping_sheet.iloc[:, 1].dropna().unique().tolist())

        indicator_note_col = params['source_cols'].get('indicator_note')
        source_note_col = params['source_cols'].get('source_note')
        
        all_comments = set()
        if indicator_note_col and indicator_note_col in df_source.columns:
            all_comments.update(df_source[indicator_note_col].dropna().astype(str))
        if source_note_col and source_note_col in df_source.columns:
            all_comments.update(df_source[source_note_col].dropna().astype(str))

        new_keywords = set()
        for comment in all_comments:
            keywords = [k.strip().lower() for k in comment.split('|')]
            for keyword in keywords:
                if keyword and keyword not in all_known_keywords:
                    new_keywords.add(keyword)
        
        return jsonify({
            'new_comments': sorted(list(new_keywords)),
            'categories': categories
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f"새로운 주석 검색 중 오류: {str(e)}"}), 500

@app.route('/export_new_rules', methods=['POST'])
def export_new_rules_route():
    try:
        if 'commentRuleFile' not in request.files:
            return jsonify({'error': '기존 주석 규칙 파일이 필요합니다.'}), 400
        
        new_rules_data = json.loads(request.form.get('new_rules'))
        
        file = request.files['commentRuleFile']
        file.seek(0)
        df_mapping_original = smart_read(file, sheet_name='최종_주석_매핑', header_row=0)
        
        file.seek(0)
        try:
            df_exception_original = smart_read(file, sheet_name='최종_주석처리_예외', header_row=0)
        except:
            df_exception_original = pd.DataFrame(columns=['예외 처리할 주석 내용'])

        new_mapping_rules = []
        for rule in new_rules_data:
            if rule.get('category') and rule['category'] != '주석처리 예외':
                new_mapping_rules.append({
                    '원본 주석 내용': rule['original'],
                    '주석 분류': rule['category'],
                    '최종 주석 내용': rule['final'],
                    '출처': '', 
                    '통계표': '' 
                })
        
        if new_mapping_rules:
            df_new_mapping = pd.DataFrame(new_mapping_rules)
            df_mapping_updated = pd.concat([df_mapping_original, df_new_mapping], ignore_index=True).drop_duplicates(subset=['원본 주석 내용'])
        else:
            df_mapping_updated = df_mapping_original

        new_exception_rules = [rule['original'] for rule in new_rules_data if rule.get('category') == '주석처리 예외']
        if new_exception_rules:
            df_new_exception = pd.DataFrame(new_exception_rules, columns=['예외 처리할 주석 내용'])
            df_exception_updated = pd.concat([df_exception_original, df_new_exception], ignore_index=True).drop_duplicates()
        else:
            df_exception_updated = df_exception_original

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_mapping_updated.to_excel(writer, sheet_name='최종_주석_매핑', index=False)
            df_exception_updated.to_excel(writer, sheet_name='최종_주석처리_예외', index=False)
        
        output.seek(0)
        return send_file(
            output,
            download_name='업데이트된_주석_규칙_파일.xlsx',
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)