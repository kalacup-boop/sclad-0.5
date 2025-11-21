import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import time
import os
import io

# Для чтения Excel по URL
import requests

# Для нечеткого сопоставления строк
from thefuzz import fuzz
from thefuzz import process

# --- КОНФИГУРАЦИЯ ---
DB_FILE = "construction_system.db"
# Минимальный порог сходства для считания материалов одинаковыми (в процентах)
FUZZY_MATCH_THRESHOLD = 80 
# Ключ для хранения URL в session_state
STOCK_URL_KEY = 'last_stock_url' 

# Список сотрудников
WORKERS_LIST = ["Выберите сотрудника...", "Хазбулат Р.", "Никулин Д.", "Волыкина Е.", "Ивонин К.", "Никонов Е.", "Губанов А.", "Яшковец В."]

st.set_page_config(page_title="Склад обьекта", layout="wide")

# --- АВТОРИЗАЦИЯ ---
def check_password():
    is_logged_in = st.session_state.get('authenticated', False)
    
    if not is_logged_in:
        params = st.query_params
        if params.get("auth") == "true":
            st.session_state['authenticated'] = True
            is_logged_in = True

    if not is_logged_in:
        st.title("🔐 Склад объекта")
        
        # --- ДВЕ КОЛОНКИ ДЛЯ ЛОГИНА И ИЗОБРАЖЕНИЯ ---
        c1, c2 = st.columns([1, 2])

        with c1:
            # Поля ввода (слева)
            username = st.text_input("Логин")
            password = st.text_input("Пароль", type="password")
            if st.button("Войти", type="primary"):
                if username == "admin" and password == "admin":
                    st.session_state['authenticated'] = True
                    st.query_params["auth"] = "true"
                    st.rerun()
                else:
                    st.error("Неверный логин или пароль")
        
        with c2:
            IMAGE_URL = "https://i.postimg.cc/wj19LcZL/photo-2025-11-21-23-59-22.jpg"
            st.image(IMAGE_URL, caption='Вас приветсвует Василий', use_container_width='true')
            
        return False
    return True

def logout():
    st.session_state['authenticated'] = False
    st.query_params.clear()
    st.rerun()

# --- ЭКСПОРТ В EXCEL ---
def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='History')
    processed_data = output.getvalue()
    return processed_data

# --- НОВАЯ ФУНКЦИЯ ДЛЯ НЕЧЕТКОГО СОПОСТАВЛЕНИЯ ---
def find_best_match(query, choices, threshold):
    """
    Находит наиболее подходящее совпадение для строки запроса (query) 
    в списке вариантов (choices) с учетом порога сходства.
    """
    # Используем extractOne для нахождения наилучшего совпадения
    result = process.extractOne(query, choices, scorer=fuzz.token_sort_ratio)
    
    if result and result[1] >= threshold:
        # result[0] - наилучшее совпадение, result[1] - балл сходства
        return result[0], result[1]
    return None, 0 # Если совпадение ниже порога, возвращаем None

# --- БАЗА ДАННЫХ ---
def get_connection():
    return sqlite3.connect(DB_FILE, check_same_thread=False)

def init_db():
    conn = get_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS projects 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS materials 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER, name TEXT, unit TEXT, planned_qty REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS shipments 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, material_id INTEGER, qty REAL, user_name TEXT, arrival_date TIMESTAMP, store TEXT, doc_number TEXT, note TEXT, op_type TEXT)''')
    
    try:
        # ПРОВЕРКА И ДОБАВЛЕНИЕ СТОЛБЦОВ
        c.execute("ALTER TABLE shipments ADD COLUMN store TEXT")
    except sqlite3.OperationalError: pass 
    try:
        c.execute("ALTER TABLE shipments ADD COLUMN doc_number TEXT")
    except sqlite3.OperationalError: pass 
    try:
        c.execute("ALTER TABLE shipments ADD COLUMN note TEXT")
    except sqlite3.OperationalError: pass 
    try:
        c.execute("ALTER TABLE shipments ADD COLUMN op_type TEXT DEFAULT 'Приход'")
        c.execute("UPDATE shipments SET op_type = 'Приход' WHERE op_type IS NULL OR op_type = ''") 
    except sqlite3.OperationalError: pass
        
    conn.commit()
    conn.close()

def update_project_name(project_id, new_name):
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("UPDATE projects SET name = ? WHERE id = ?", (new_name, int(project_id)))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def get_projects():
    conn = get_connection()
    try:
        df = pd.read_sql("SELECT * FROM projects", conn)
    except:
        df = pd.DataFrame()
    conn.close()
    return df

def add_project(name):
    conn = get_connection()
    try:
        c = conn.cursor()
        c.execute("INSERT INTO projects (name) VALUES (?)", (name,))
        conn.commit()
        return True
    except:
        return False
    finally:
        conn.close()

def delete_specific_project(project_id):
    conn = get_connection()
    c = conn.cursor()
    pid = int(project_id)
    c.execute("DELETE FROM shipments WHERE material_id IN (SELECT id FROM materials WHERE project_id=?)", (pid,))
    c.execute("DELETE FROM materials WHERE project_id=?", (pid,))
    c.execute("DELETE FROM projects WHERE id=?", (pid,))
    conn.commit()
    conn.close()

def clear_project_history(project_id):
    conn = get_connection()
    c = conn.cursor()
    pid = int(project_id)
    c.execute("DELETE FROM shipments WHERE material_id IN (SELECT id FROM materials WHERE project_id=?)", (pid,))
    conn.commit()
    conn.close()

def load_excel_final(project_id, df):
    conn = get_connection()
    c = conn.cursor()
    pid = int(project_id)
    c.execute("DELETE FROM materials WHERE project_id=?", (pid,))
    
    success = 0
    log = []
    for i, row in df.iterrows():
        try:
            name = str(row.iloc[0]).strip()
            unit = str(row.iloc[1]).strip()
            qty_str = str(row.iloc[2]).replace(',', '.').replace('\xa0', '').strip()
            try:
                qty = float(qty_str)
            except:
                qty = 0.0

            if name and name.lower() != 'nan':
                c.execute("INSERT INTO materials (project_id, name, unit, planned_qty) VALUES (?, ?, ?, ?)",
                          (pid, name, unit, qty))
                success += 1
        except Exception as e:
            log.append(f"Ошибка строки {i}: {e}")
            
    conn.commit()
    conn.close()
    return success, log

def add_shipment(material_id, qty, user, date, store, doc_number, note, op_type='Приход'):
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO shipments (material_id, qty, user_name, arrival_date, store, doc_number, note, op_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
              (int(material_id), float(qty), user, date, store, doc_number, note, op_type))
    shipment_id = c.lastrowid
    conn.commit()
    conn.close()
    return shipment_id

def undo_shipment(shipment_id, current_user):
    conn = get_connection()
    c = conn.cursor()
    
    c.execute("SELECT material_id, qty, store, doc_number, note FROM shipments WHERE id = ?", (shipment_id,))
    original_data = c.fetchone()
    
    if original_data:
        material_id, qty, store, doc_number, note = original_data
        
        cancel_qty = -abs(qty) 
        
        c.execute("INSERT INTO shipments (material_id, qty, user_name, arrival_date, store, doc_number, note, op_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                  (material_id, cancel_qty, current_user, datetime.now(), store, doc_number, f"ОТМЕНА операции ID:{shipment_id}. Оригинальное Примечание: {note}", 'Отмена'))
        
        conn.commit()
        conn.close()
        return True
    
    conn.close()
    return False

def get_data(project_id):
    conn = get_connection()
    pid = int(project_id)
    materials = pd.read_sql("SELECT * FROM materials WHERE project_id=?", conn, params=(pid,))
    
    if materials.empty:
        conn.close()
        return pd.DataFrame(), pd.DataFrame()

    ids = materials['id'].tolist()
    if not ids:
        conn.close()
        return materials, pd.DataFrame()
        
    ids_placeholder = ','.join(['?'] * len(ids))
    
    history = pd.read_sql(f"""
        SELECT 
            s.id, 
            m.name as 'Материал', 
            s.qty as 'Кол-во', 
            s.op_type as 'Тип опер.', 
            s.user_name as 'Кто', 
            s.store as 'Магазин', 
            s.doc_number as '№ Док.', 
            s.note as 'Примечание', 
            s.arrival_date as 'Дата'
        FROM shipments s 
        JOIN materials m ON s.material_id = m.id
        WHERE m.id IN ({ids_placeholder}) 
        ORDER BY s.arrival_date DESC
    """, conn, params=ids)
    
    sums = pd.read_sql(f"""
        SELECT material_id, SUM(qty) as total 
        FROM shipments 
        WHERE material_id IN ({ids_placeholder}) 
        GROUP BY material_id
    """, conn, params=ids)
    
    conn.close()
    
    full = pd.merge(materials, sums, left_on='id', right_on='material_id', how='left')
    full['total'] = full['total'].fillna(0)
    full['prog'] = full.apply(lambda x: x['total']/x['planned_qty'] if x['planned_qty']>0 else 0, axis=1)
    
    return full, history

def submit_entry_callback(material_id, qty, user, input_key, current_pid, store, doc_number, note):
    # 1. Проверка
    if user == "Выберите сотрудника..." or not user:
        st.toast("⚠️ Ошибка: Выберите фамилию сотрудника!", icon="❌")
        return

    if qty <= 0:
        st.toast("⚠️ Ошибка: Количество должно быть больше 0!", icon="❌")
        return

    # 2. Сохранение
    try:
        shipment_id = add_shipment(material_id, qty, user, datetime.now(), store, doc_number, note, op_type='Приход') 
        st.toast("✅ Данные успешно внесены!", icon="💾")
        
        st.session_state['last_shipment_id'] = shipment_id
        st.session_state['last_shipment_pid'] = current_pid 
        st.session_state['current_user'] = user 
        
        # 3. Сброс значения поля ввода
        st.session_state[input_key] = 0.0
        
    except Exception as e:
        st.toast(f"Ошибка записи: {e}", icon="🔥")

# ФУНКЦИЯ ДЛЯ СОПОСТАВЛЕНИЯ
def compare_with_stock_excel(file_source, data_df):
    
    stock_df = pd.DataFrame()
    
    # 1. Загрузка файла по URL/Google Sheets/file_uploader
    if isinstance(file_source, str):
        original_url = file_source.strip()
        
        if "docs.google.com/spreadsheets/d/" in original_url and "/edit" in original_url:
            st.info("🔗 Обнаружена ссылка на Google Таблицу. Преобразование в ссылку для экспорта...")
            try:
                # Извлекаем ID
                start_index = original_url.find('/d/') + 3
                end_index = original_url.find('/edit')
                sheet_id = original_url[start_index:end_index]
                file_source = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
            except Exception as e:
                st.error(f"Ошибка при обработке URL Google Таблицы: {e}")
                return pd.DataFrame()
        
        st.info(f"⏳ Загрузка данных по URL...")
        try:
            response = requests.get(file_source)
            response.raise_for_status() 
            stock_df = pd.read_excel(io.BytesIO(response.content), header=None)
            st.success("✅ Файл успешно загружен.")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                st.error("Ошибка 403 (Доступ запрещен). Проверьте общий доступ к Google Таблице.")
            else:
                 st.error(f"Ошибка при загрузке по URL: Проверьте, что ссылка корректна и файл доступен. Ошибка: {e}")
            return pd.DataFrame()
        except requests.exceptions.RequestException as e:
            st.error(f"Ошибка соединения/загрузки: {e}")
            return pd.DataFrame()
            
    else:
        # Это не должно происходить в текущей логике, но для безопасности
        st.error("Непредвиденный источник файла.")
        return pd.DataFrame()
    
    # --- ЛОГИКА СОПОСТАВЛЕНИЯ С FUZZY MATCH ---
    
    # 2. Проверка количества столбцов
    MIN_COLS = 17 
    if stock_df.shape[1] < MIN_COLS:
        st.error(f"⚠️ Ошибка: В файле должно быть минимум {MIN_COLS} столбцов. Найдено: {stock_df.shape[1]}")
        return pd.DataFrame()
        
    # 3. Переименование и подготовка данных
    stock_df.rename(columns={
        1: 'Name_Stock',
        12: 'Store_Stock',
        13: 'Qty_Stock',
        16: 'Shelf_Stock' 
    }, inplace=True)
    
    # Очищаем данные запасов
    stock_df_cleaned = stock_df[['Name_Stock', 'Store_Stock', 'Qty_Stock', 'Shelf_Stock']].copy()
    stock_df_cleaned.dropna(subset=['Name_Stock'], inplace=True)
    
    # Список уникальных названий для поиска (в нижнем регистре)
    stock_names_list_lower = stock_df_cleaned['Name_Stock'].astype(str).str.strip().str.lower().unique().tolist()
    
    # Подготовка результирующего DataFrame
    project_materials = data_df[['name', 'unit']].copy()
    project_materials.rename(columns={'name': 'Name_Project'}, inplace=True)
    project_materials['Name_Project_Lower'] = project_materials['Name_Project'].astype(str).str.strip().str.lower()
    
    # Добавляем колонки для результатов сопоставления
    project_materials['Name_Stock_Match'] = None
    project_materials['Match_Score'] = 0
    
    # 4. Выполнение нечеткого сопоставления
    st.info(f"🔎 Запуск нечеткого сопоставления с порогом **{FUZZY_MATCH_THRESHOLD}%**...")
    
    # Создаем словарь, чтобы избежать повторного поиска для одинаковых совпадений
    matched_stock_data = {} 
    
    for index, row in project_materials.iterrows():
        project_name = row['Name_Project_Lower']
        
        # Находим лучшее совпадение
        best_match, score = find_best_match(project_name, stock_names_list_lower, FUZZY_MATCH_THRESHOLD)
        
        if score > 0:
            project_materials.at[index, 'Name_Stock_Match'] = best_match
            project_materials.at[index, 'Match_Score'] = score
            
            # Сохраняем данные из исходного DF, чтобы потом объединить
            if best_match not in matched_stock_data:
                # Находим все строки в исходном DF, соответствующие наилучшему совпадению (без учета регистра)
                match_data = stock_df_cleaned[stock_df_cleaned['Name_Stock'].astype(str).str.strip().str.lower() == best_match]
                
                # Если совпадений несколько (на разных складах), агрегируем
                total_qty = match_data['Qty_Stock'].sum()
                # Объединяем склады и полки через запятую
                all_stores = match_data['Store_Stock'].astype(str).str.cat(sep='; ')
                all_shelves = match_data['Shelf_Stock'].astype(str).str.cat(sep='; ')
                
                # Храним агрегированные данные
                matched_stock_data[best_match] = {
                    'Qty_Stock_Agg': total_qty,
                    'Store_Stock_Agg': all_stores,
                    'Shelf_Stock_Agg': all_shelves
                }

    # 5. Объединение результатов
    
    matched_df = pd.DataFrame.from_dict(matched_stock_data, orient='index').reset_index()
    matched_df.rename(columns={'index': 'Name_Stock_Match'}, inplace=True)
    
    final_df = pd.merge(
        project_materials, 
        matched_df, 
        on='Name_Stock_Match', 
        how='left'
    )
    
    # 6. Очистка и форматирование результата
    result_df = final_df[[
        'Name_Project', 
        'unit', 
        'Qty_Stock_Agg', 
        'Store_Stock_Agg',
        'Shelf_Stock_Agg',
        'Match_Score'
    ]].drop_duplicates(subset=['Name_Project'])
    
    result_df.columns = ['Материал (План)', 'Ед. изм.', 'Количество (Склад)', 'Склады', 'Номера полок', 'Сходство (%)']
    
    # Заполнение пустых значений
    result_df['Количество (Склад)'] = result_df['Количество (Склад)'].fillna(0).astype(float).round(2)
    result_df['Склады'] = result_df['Склады'].fillna('—')
    result_df['Номера полок'] = result_df['Номера полок'].fillna('—') 
    
    return result_df.sort_values(by=['Сходство (%)', 'Материал (План)'], ascending=[False, True])

# --- ЛОГИКА ПРИЛОЖЕНИЯ ---

if not check_password():
    st.stop()

init_db()

# --- САЙДБАР (Без изменений) ---
with st.sidebar:
    st.header("📂 Управление объектами")
    new_name = st.text_input("Имя нового объекта")
    if st.button("Добавить объект"):
        if new_name:
            if add_project(new_name):
                st.success("Создано!")
                time.sleep(0.5)
                st.rerun()
            else:
                st.error("Такое имя уже есть")
    
    st.divider()
    
    # --- БЛОК РЕЗЕРВНОГО КОПИРОВАНИЯ ---
    with st.expander("💾 Резервное копирование"):
        st.info("Для настройки еженедельных бэкапов используйте внешний планировщик задач (cron) на сервере.")
        st.write("**1. Скачать всю базу**")
        
        if os.path.exists(DB_FILE):
            with open(DB_FILE, "rb") as f:
                db_bytes = f.read()
            
            st.download_button(
                label="⬇️ Скачать базу (.db)",
                data=db_bytes,
                file_name=f"backup_{datetime.now().strftime('%Y%m%d_%H%M')}.db",
                mime="application/octet-stream"
            )
        else:
            st.error("База данных еще не создана.")

        st.divider()
        st.write("**2. Восстановить из копии**")
        uploaded_db = st.file_uploader("Загрузите файл .db", type=['db'])
        
        if uploaded_db:
            st.warning("⚠️ Это действие полностью заменит текущие данные!")
            if st.button("🔄 Заменить текущую базу", type="primary"):
                with open(DB_FILE, "wb") as f:
                    f.write(uploaded_db.getbuffer())
                st.success("База данных восстановлена!")
                time.sleep(1)
                st.rerun()

    st.divider()
    if st.button("Выйти из аккаунта"):
        logout()

# --- ОСНОВНОЕ ОКНО ---
st.title("🏗️ Склад обьекта")

projects = get_projects()

if projects.empty:
    st.info("Список объектов пуст. Добавьте первый объект в меню слева.")
else:
    project_tabs_names = [f"🛠️ {name}" for name in projects['name'].tolist()]
    tabs = st.tabs(project_tabs_names)
    
    for i, tab in enumerate(tabs):
        pid = int(projects.iloc[i]['id'])
        pname = projects.iloc[i]['name']
        
        st.session_state['current_pid'] = pid 
        
        with tab:
            # --- СЕКЦИЯ НАСТРОЕК (с возможностью редактирования) ---
            with st.expander("⚙️ Настройки / Удаление объекта"):
                # --- БЛОК РЕДАКТИРОВАНИЯ НАЗВАНИЯ ---
                st.write("**Редактирование названия**")
                new_pname = st.text_input("Новое название объекта", value=pname, key=f"edit_name_{pid}")
                if st.button("📝 Сохранить название", key=f"save_name_{pid}", type="secondary"):
                    if new_pname and new_pname != pname:
                        if update_project_name(pid, new_pname):
                            st.toast("Название обновлено!")
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error("Ошибка: Такое название уже используется.")
                    else:
                        st.warning("Название не изменилось или пусто.")
                st.divider()

                # --- БЛОК СБРОСА И УДАЛЕНИЯ ---
                col_del1, col_del2 = st.columns(2)
                
                confirm_reset_key = f"confirm_reset_{pid}"
                confirm_delete_key = f"confirm_delete_{pid}"

                with col_del1:
                    st.write("**Сброс данных** (только история)")
                    if not st.session_state.get(confirm_reset_key, False):
                        if st.button("🧹 Сбросить историю", key=f"pre_reset_{pid}"):
                            st.session_state[confirm_reset_key] = True
                            st.rerun()
                    else:
                        st.warning("Вы уверены?")
                        col_yes, col_no = st.columns(2)
                        if col_yes.button("ДА, СБРОСИТЬ", key=f"yes_reset_{pid}", type="primary"):
                            clear_project_history(pid)
                            st.session_state[confirm_reset_key] = False
                            st.toast("История очищена!", icon="↩️")
                            time.sleep(1)
                            st.rerun()
                        if col_no.button("Отмена", key=f"no_reset_{pid}"):
                            st.session_state[confirm_reset_key] = False
                            st.rerun()
                
                with col_del2:
                    st.write("**Удаление объекта** (полное)")
                    if not st.session_state.get(confirm_delete_key, False):
                        if st.button("❌ Удалить объект", key=f"pre_del_{pid}"):
                            st.session_state[confirm_delete_key] = True
                            st.rerun()
                    else:
                        st.error("ВНИМАНИЕ: Все данные будут удалены!")
                        col_yes_d, col_no_d = st.columns(2)
                        if col_yes_d.button("ДА, УДАЛИТЬ", key=f"yes_del_{pid}", type="primary"):
                            delete_specific_project(pid)
                            st.session_state[confirm_delete_key] = False
                            st.success("Объект удален")
                            time.sleep(1)
                            st.rerun()
                        if col_no_d.button("Отмена", key=f"no_del_{pid}"):
                            st.session_state[confirm_delete_key] = False
                            st.rerun()
            
            # --- ДАННЫЕ ---
            data_df, hist_df = get_data(pid)
            
            plan_upload_key = f"u_{pid}"
            plan_confirm_key = f"plan_confirm_{pid}"
            
            is_expanded = data_df.empty or st.session_state.get(plan_confirm_key, False)
            
            with st.expander("📥 Обновить план (Excel)", expanded=is_expanded):
                uploaded_file = st.file_uploader(f"Файл для '{pname}'", type='xlsx', key=plan_upload_key)
                
                if uploaded_file:
                    
                    can_load = st.session_state.get(plan_confirm_key, False) or data_df.empty
                    
                    if not can_load:
                        st.warning("⚠️ Внимание: Загрузка нового файла заменит текущий **ПЛАН** (список материалов), но вся история приходов **будет СОХРАНЕНА**.")
                        if st.button("ПОДТВЕРДИТЬ И ЗАГРУЗИТЬ", key=f"confirm_load_{pid}", type="primary"):
                            st.session_state[plan_confirm_key] = True
                            st.rerun() 
                    
                    if can_load:
                        if st.button("ЗАПИСАТЬ В БАЗУ", key=f"btn_{pid}", type="primary"):
                            df_preview = pd.read_excel(uploaded_file)
                            cnt, errs = load_excel_final(pid, df_preview)
                            st.session_state[plan_confirm_key] = False
                            st.success(f"Обновлено: {cnt} строк")
                            time.sleep(1)
                            st.rerun()

            if not data_df.empty:
                # --- ОБЩАЯ ШКАЛА ---
                st.divider()
                total_planned = data_df['planned_qty'].sum()
                total_shipped = data_df['total'].sum()
                
                if total_planned > 0:
                    overall_percent = total_shipped / total_planned
                else:
                    overall_percent = 0.0
                
                bar_value = min(overall_percent, 1.0)
                st.subheader("Общий прогресс по объекту")
                st.progress(bar_value, text=f"Выполнение: {overall_percent:.1%} (Всего принято: {total_shipped:.1f} / План: {total_planned:.1f})")
                
                st.divider()

                # --- ВВОД ПРИХОДА (ТОЛЬКО ПОЛЯ) ---
                st.subheader("Ввод прихода")
                
                c1, c2, c3 = st.columns([3, 1, 2])
                
                opts = dict(zip(data_df['name'], data_df['id']))
                
                with c1:
                    s_name = st.selectbox("Материал", list(opts.keys()), key=f"sel_{pid}")
                    s_id = opts[s_name]
                    curr = data_df[data_df['id']==s_id].iloc[0]
                    st.caption(f"План: {curr['planned_qty']} {curr['unit']} | Факт: {curr['total']}")
                    
                input_key = f"num_{pid}"
                
                with c2:
                    val = st.number_input("Кол-во", min_value=0.0, step=1.0, key=input_key)
                
                with c3:
                    who = st.selectbox("Кто принял", WORKERS_LIST, key=f"who_{pid}")
                
                # --- СКРЫТИЕ ДОПОЛНИТЕЛЬНЫХ ПОЛЕЙ ПОД EXPANDER ---
                with st.expander("📝 Дополнительные данные (Магазин, Док. №, Прим.)"):
                    r2_c1, r2_c2 = st.columns(2)
                    
                    with r2_c1:
                        store_input = st.text_input("Магазин / Поставщик", key=f"store_{pid}", value=st.session_state.get(f"store_{pid}", ""))

                    with r2_c2:
                        doc_input = st.text_input("Номер документа", key=f"doc_{pid}", value=st.session_state.get(f"doc_{pid}", ""))
                        
                    note_input = st.text_area("Примечание", height=50, key=f"note_{pid}", value=st.session_state.get(f"note_{pid}", ""))
                    
                if f"store_{pid}" not in st.session_state: st.session_state[f"store_{pid}"] = ""
                if f"doc_{pid}" not in st.session_state: st.session_state[f"doc_{pid}"] = ""
                if f"note_{pid}" not in st.session_state: st.session_state[f"note_{pid}"] = ""
                
                # --- БЛОК КНОПОК ПЕРЕМЕЩЕН СЮДА (ПОСЛЕ ДОП. ДАННЫХ) ---
                st.divider()
                st.subheader("Управление операцией")
                
                btn_c1, btn_c2 = st.columns([1, 1])
                
                show_undo = st.session_state.get('last_shipment_id') and st.session_state.get('last_shipment_pid') == pid
                current_user = st.session_state.get('current_user', 'Система')
                
                with btn_c1:
                    st.button("Внести (записать приход)", 
                              key=f"ok_{pid}", 
                              type="primary",
                              use_container_width=True, 
                              on_click=submit_entry_callback,
                              args=(s_id, val, who, input_key, pid, st.session_state.get(f"store_{pid}", ""), st.session_state.get(f"doc_{pid}", ""), st.session_state.get(f"note_{pid}", "")) 
                              )
                
                with btn_c2:
                    if st.button("↩️ Отменить последний ввод", 
                                 key=f"undo_{pid}", 
                                 type="secondary",
                                 disabled=not show_undo, 
                                 use_container_width=True
                                 ):
                        
                        undo_shipment(st.session_state['last_shipment_id'], current_user)
                        
                        del st.session_state['last_shipment_id']
                        del st.session_state['last_shipment_pid']
                        st.toast("Последний приход отменен и добавлен в историю!", icon="↩️")
                        time.sleep(0.5)
                        st.rerun()
                
                # --- НОВЫЙ БЛОК: Сравнение с фактическими остатками (С СОХРАНЕНИЕМ ССЫЛКИ) ---
                st.divider()
                
                with st.expander("🔍 **Сравнение с фактическими остатками склада (по URL)**"):
                    #st.warning(f"Чтобы использовать нечеткое сопоставление, необходимо установить библиотеки: `pip install fuzzywuzzy python-levenshtein`")
                    st.info(f"Сравнение будет произведено с порогом сходства **{FUZZY_MATCH_THRESHOLD}%**.")
                    
                    col_url, col_btn = st.columns([4, 1])
                    
                    current_url = st.session_state.get(STOCK_URL_KEY, "")
                    
                    with col_url:
                        # Поле ввода, инициализируемое сохраненным значением
                        new_url = st.text_input(
                            "URL-ссылка на Excel/Google Таблицу", 
                            value=current_url, 
                            key=f"input_url_{pid}",
                            help="Вставьте ссылку Google Таблицы или прямую ссылку на Excel-файл. Нажмите 'Сохранить и сравнить', чтобы записать ее."
                        )
                        
                    with col_btn:
                        st.text(" ") # Визуальный отступ
                        if st.button("💾 Сохранить и сравнить", key=f"save_compare_btn_{pid}", type="primary", use_container_width=True):
                            if new_url:
                                st.session_state[STOCK_URL_KEY] = new_url # Сохраняем новую ссылку
                                st.session_state['trigger_compare'] = new_url
                                st.rerun()
                            else:
                                st.error("Поле ссылки не может быть пустым.")
                    
                    # --- КНОПКА ОБНОВЛЕНИЯ ПО СОХРАНЕННОЙ ССЫЛКЕ ---
                    if current_url:
                        st.markdown("---")
                        st.success(f"Текущая сохраненная ссылка: **{current_url[:60]}...**")
                        
                        if st.button("🔄 Обновить данные по сохраненной ссылке", key=f"refresh_compare_btn_{pid}", type="secondary", use_container_width=True):
                            st.session_state['trigger_compare'] = current_url
                            st.rerun()

                    # --- ЛОГИКА ОТОБРАЖЕНИЯ РЕЗУЛЬТАТОВ (ВЫПОЛНЯЕТСЯ ПОСЛЕ RERUN) ---
                    if st.session_state.get('trigger_compare'):
                        url_to_use = st.session_state.pop('trigger_compare')
                        
                        if data_df.empty:
                            st.error("Сначала загрузите план материалов для текущего объекта.")
                        else:
                            with st.spinner('Обработка файла и нечеткое сопоставление...'):
                                comparison_result = compare_with_stock_excel(url_to_use, data_df)
                            
                            if not comparison_result.empty:
                                
                                found_df = comparison_result[comparison_result['Склады'] != '—']
                                not_found_df = comparison_result[comparison_result['Склады'] == '—']
                                
                                st.subheader(f"✅ Найдено совпадений: {len(found_df)} из {len(comparison_result)}")
                                st.dataframe(found_df, use_container_width=True)
                                
                                if not not_found_df.empty:
                                    st.subheader(f"❌ Материалы из плана, не найденные в файле остатков:")
                                    st.dataframe(not_found_df.drop(columns=['Количество (Склад)', 'Склады', 'Номера полок', 'Сходство (%)']), use_container_width=True)


                
                # --- ДЕТАЛИЗАЦИЯ (СКРЫТАЯ) ---
                st.divider()
                
                with st.expander("📊 Детализация (Остатки) — Нажмите, чтобы развернуть", expanded=False):
                    
                    data_df = data_df.sort_values(by=['prog', 'name'], ascending=[False, True])
                    
                    for index, row in data_df.iterrows():
                        if row['prog'] >= 1.0:
                            icon = "✅"
                        elif row['prog'] > 0:
                            icon = "⏳"
                        else:
                            icon = "⚪"
                        
                        label = f"{icon} {row['name']} — {row['prog']:.0%}"
                        
                        with st.expander(label):
                            c_det1, c_det2, c_det3 = st.columns(3)
                            with c_det1:
                                st.caption("Ед. изм.")
                                st.write(row['unit'])
                            with c_det2:
                                st.caption("План")
                                st.write(f"{row['planned_qty']:.2f}")
                            with c_det3:
                                st.caption("Факт")
                                st.write(f"{row['total']:.2f}")
                            
                            ostalos = row['planned_qty'] - row['total']
                            if ostalos > 0:
                                st.info(f"Осталось принять: {ostalos:.2f} {row['unit']}")
                            elif ostalos < 0:
                                st.warning(f"Перерасход: {abs(ostalos):.2f} {row['unit']}")
                            else:
                                st.success("План выполнен!")

                # --- ИСТОРИЯ ---
                if not hist_df.empty:
                    st.divider()
                    with st.expander("📜 История операций (Скачать)"):
                        
                        def format_qty_and_type(row):
                            qty = row['Кол-во']
                            op_type = row['Тип опер.']
                            
                            if op_type == 'Отмена':
                                color = 'red'
                                qty_str = f"- {abs(qty):.2f}"
                            elif op_type == 'Приход' and qty > 0:
                                color = 'green'
                                qty_str = f"+ {qty:.2f}"
                            else:
                                color = 'black'
                                qty_str = f"{qty:.2f}"
                                
                            return f"<span style='color: {color}; font-weight: bold;'>{qty_str}</span>"

                        
                        display_df = hist_df.copy()
                        display_df['Кол-во'] = display_df.apply(format_qty_and_type, axis=1)

                        st.markdown(display_df.drop(columns=['id', 'Тип опер.']).to_html(escape=False, index=False), unsafe_allow_html=True)
                        
                        excel_data = to_excel(hist_df.drop(columns=['id']))
                        st.download_button(
                            label="📥 Скачать историю (Excel)",
                            data=excel_data,
                            file_name=f"История_{pname}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_{pid}"
                        )




