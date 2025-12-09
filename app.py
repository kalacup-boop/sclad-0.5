import streamlit as st
import pandas as pd
from datetime import datetime
import time
import io
import requests
from thefuzz import fuzz
from thefuzz import process

# --- КОНФИГУРАЦИЯ ---
FUZZY_MATCH_THRESHOLD = 80
STOCK_URL_KEY = 'last_stock_url'
WORKERS_LIST = ["Выберите сотрудника...", "Хазбулат Р.", "Никулин Д.", "Волыкина Е.", "Ивонин К.", "Никонов Е.", "Губанов А.", "Яшковец В."]

st.set_page_config(page_title="Склад обьекта", layout="wide")

# #######################################################
# 🚀 SUPABASE / POSTGRESQL КОННЕКТОР
# #######################################################

try:
    # 🚨 ВРЕМЕННЫЙ ДИАГНОСТИЧЕСКИЙ ТЕСТ: ИСПОЛЬЗУЕМ ПАРАМЕТРЫ НАПРЯМУЮ 🚨
    # Это позволяет полностью обойти чтение secrets.toml
    conn = st.connection(
        "supabase",  # Временное имя
        type="sql",
        url="postgresql://postgres:.z4._bQNf85quP*@db.nmqihnlcdqysngirqwba.supabase.co:5432/postgres"
    )
    # Если тест успешен, это сообщение увидим вместо ошибки
    # st.success("✅ Подключение к Supabase успешно (тест bypass).") 
    
except Exception as e:
    st.error(f"❌ Ошибка подключения к базе данных Supabase. Проверьте настройки secrets.toml и статус проекта: {e}")
    # st.stop() 
    pass

# --- АВТОРИЗАЦИЯ (Без изменений) ---
def check_password():
    is_logged_in = st.session_state.get('authenticated', False)
    # ... (Логика авторизации) ...
    if not is_logged_in:
        params = st.query_params
        if params.get("auth") == "true":
            st.session_state['authenticated'] = True
            is_logged_in = True

    if not is_logged_in:
        st.title("🔐Склад объекта")
        
        c1, c2 = st.columns([1, 2])

        with c1:
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
            IMAGE_URL = "https://i.postimg.cc/3rLM10gN/photo-2025-11-21-23-59-22-Photoroom.png"
            st.image(IMAGE_URL, caption='Сделано в Gemini', use_container_width='true')
            
        return False
    return True

def logout():
    st.session_state['authenticated'] = False
    st.query_params.clear()
    st.rerun()

# --- ФУНКЦИИ УТИЛИТ (Без изменений) ---
def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='History')
    processed_data = output.getvalue()
    return processed_data

def find_best_match(query, choices, threshold):
    result = process.extractOne(query, choices, scorer=fuzz.token_sort_ratio)
    
    if result and result[1] >= threshold:
        return result[0], result[1]
    return None, 0

# #######################################################
# 💾 ФУНКЦИИ УПРАВЛЕНИЯ БАЗОЙ ДАННЫХ (PostgreSQL)
# #######################################################

def init_db():
    # Создание таблиц (PostgreSQL синтаксис)
    try:
        conn.query('''
            CREATE TABLE IF NOT EXISTS projects (
                id SERIAL PRIMARY KEY, 
                name TEXT UNIQUE NOT NULL
            )
        ''', result='auto')
        conn.query('''
            CREATE TABLE IF NOT EXISTS materials (
                id SERIAL PRIMARY KEY, 
                project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE, 
                name TEXT NOT NULL, 
                unit TEXT, 
                planned_qty REAL NOT NULL
            )
        ''', result='auto')
        conn.query('''
            CREATE TABLE IF NOT EXISTS shipments (
                id SERIAL PRIMARY KEY, 
                material_id INTEGER REFERENCES materials(id) ON DELETE CASCADE, 
                qty REAL NOT NULL, 
                user_name TEXT, 
                arrival_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
                store TEXT, 
                doc_number TEXT, 
                note TEXT, 
                op_type TEXT NOT NULL
            )
        ''', result='auto')
    except Exception as e:
        # В случае ошибки подключения/создания, просто пропускаем
        pass

@st.cache_data(ttl=300)
def get_projects():
    # Используем TTL=0 для некэшированного чтения при первом запуске
    df = conn.query("SELECT * FROM projects ORDER BY name", ttl=0)
    return df

def add_project(name):
    try:
        conn.query("INSERT INTO projects (name) VALUES (%(name)s)", params={"name": name}, result='auto')
        st.cache_data.clear()
        return True
    except:
        return False

def update_project_name(project_id, new_name):
    try:
        conn.query("UPDATE projects SET name = %(new_name)s WHERE id = %(id)s", params={"new_name": new_name, "id": int(project_id)}, result='auto')
        st.cache_data.clear()
        return True
    except:
        return False

def delete_specific_project(project_id):
    pid = int(project_id)
    # CASCADE удалит связанные материалы и их приходы
    conn.query("DELETE FROM projects WHERE id = %(pid)s", params={"pid": pid}, result='auto')
    st.cache_data.clear()

def clear_project_history(project_id):
    pid = int(project_id)
    # Удаляем только приходы, связанные с материалами этого проекта
    conn.query("DELETE FROM shipments WHERE material_id IN (SELECT id FROM materials WHERE project_id=%(pid)s)", params={"pid": pid}, result='auto')
    st.cache_data.clear()

def load_excel_final(project_id, df):
    pid = int(project_id)
    # 1. Удаляем все старые материалы, чтобы обновить план
    conn.query("DELETE FROM materials WHERE project_id = %(pid)s", params={"pid": pid}, result='auto')
    
    success = 0
    log = []
    insert_data = []
    
    # 2. Подготавливаем данные для массовой вставки
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
                insert_data.append({"project_id": pid, "name": name, "unit": unit, "planned_qty": qty})
                success += 1
        except Exception as e:
            log.append(f"Ошибка строки {i}: {e}")
            
    # 3. Массовая вставка (более эффективна для PostgreSQL)
    if insert_data:
        insert_df = pd.DataFrame(insert_data)
        conn.insert(insert_df, table="materials", if_exists='append')
    
    st.cache_data.clear()
    return success, log

def add_shipment(material_id, qty, user, date, store, doc_number, note, op_type='Приход'):
    # Вставка нового прихода
    conn.query(
        """
        INSERT INTO shipments 
        (material_id, qty, user_name, arrival_date, store, doc_number, note, op_type) 
        VALUES (%(material_id)s, %(qty)s, %(user)s, %(date)s, %(store)s, %(doc_number)s, %(note)s, %(op_type)s)
        """,
        params={"material_id": int(material_id), "qty": float(qty), "user": user, "date": date, "store": store, "doc_number": doc_number, "note": note, "op_type": op_type},
        result='auto'
    )
    return True 

def undo_shipment(shipment_id, current_user):
    # Получаем данные последней операции
    original_data_df = conn.query("SELECT id, material_id, qty, store, doc_number, note FROM shipments WHERE id = %(shipment_id)s",
                                 params={"shipment_id": shipment_id}, ttl=0)
    
    if not original_data_df.empty:
        original_data = original_data_df.iloc[0]
        material_id = original_data['material_id']
        # Инвертируем количество для отмены
        cancel_qty = -abs(original_data['qty'])
        
        # Записываем операцию "Отмена"
        conn.query(
            """
            INSERT INTO shipments 
            (material_id, qty, user_name, arrival_date, store, doc_number, note, op_type) 
            VALUES (%(material_id)s, %(qty)s, %(user)s, %(date)s, %(store)s, %(doc_number)s, %(note)s, 'Отмена')
            """,
            params={
                "material_id": material_id, "qty": cancel_qty, "user": current_user, "date": datetime.now(), 
                "store": original_data['store'], "doc_number": original_data['doc_number'], 
                "note": f"ОТМЕНА операции ID:{shipment_id}. Оригинальное Примечание: {original_data['note']}"
            },
            result='auto'
        )
        st.cache_data.clear()
        return True
    return False

@st.cache_data(ttl=5)
def get_data(project_id):
    pid = int(project_id)
    
    # Запрос для получения плана и факта (JOIN и SUM)
    full_df = conn.query("""
        SELECT m.id, m.name, m.unit, m.planned_qty, COALESCE(SUM(s.qty), 0) AS total
        FROM materials m
        LEFT JOIN shipments s ON m.id = s.material_id
        WHERE m.project_id = %(pid)s
        GROUP BY m.id, m.name, m.unit, m.planned_qty
        ORDER BY m.name
    """, params={"pid": pid})
    
    # Запрос для получения истории операций (TO_CHAR для форматирования даты в PostgreSQL)
    history_df = conn.query("""
        SELECT s.id, m.name AS "Материал", s.qty AS "Кол-во", s.op_type AS "Тип опер.", s.user_name AS "Кто", 
               s.store AS "Магазин", s.doc_number AS "№ Док.", s.note AS "Примечание", 
               TO_CHAR(s.arrival_date, 'DD.MM.YYYY HH24:MI') AS "Дата"
        FROM shipments s 
        JOIN materials m ON s.material_id = m.id
        WHERE m.project_id = %(pid)s
        ORDER BY s.arrival_date DESC
    """, params={"pid": pid})
    
    if full_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    full_df['prog'] = full_df.apply(lambda x: x['total'] / x['planned_qty'] if x['planned_qty'] > 0 else 0, axis=1)
    
    return full_df, history_df

def submit_entry_callback(material_id, qty, user, input_key, current_pid, store, doc_number, note):
    if user == "Выберите сотрудника..." or not user:
        st.toast("⚠️ Ошибка: Выберите фамилию сотрудника!", icon="❌")
        return

    if qty <= 0:
        st.toast("⚠️ Ошибка: Количество должно быть больше 0!", icon="❌")
        return

    try:
        add_shipment(material_id, qty, user, datetime.now(), store, doc_number, note, op_type='Приход') 
        st.toast("✅ Данные успешно внесены!", icon="💾")
        
        # Находим ID последней операции для функционала "Отмена"
        latest_id_df = conn.query("SELECT id FROM shipments ORDER BY id DESC LIMIT 1", ttl=0)
        latest_shipment_id = latest_id_df.iloc[0]['id'] if not latest_id_df.empty else None

        st.session_state['last_shipment_id'] = latest_shipment_id
        st.session_state['last_shipment_pid'] = current_pid 
        st.session_state['current_user'] = user 
        
        st.cache_data.clear()
        st.session_state[input_key] = 0.0
        
    except Exception as e:
        st.toast(f"Ошибка записи: {e}", icon="🔥")

# --- ФУНКЦИЯ ДЛЯ СОПОСТАВЛЕНИЯ (Без изменений) ---
def compare_with_stock_excel(file_source, data_df):
    
    stock_df = pd.DataFrame()
    
    # 1. Загрузка файла по URL
    if isinstance(file_source, str):
        original_url = file_source.strip()
        
        # Автоматическая конвертация ссылки Google Таблиц
        if "docs.google.com/spreadsheets/d/" in original_url and "/edit" in original_url:
            try:
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
        except Exception as e:
            st.error(f"Ошибка при загрузке по URL: {e}")
            return pd.DataFrame()
            
    else:
        st.error("Непредвиденный источник файла.")
        return pd.DataFrame()
    
    # --- ЛОГИКА СОПОСТАВЛЕНИЯ С FUZZY MATCH ---
    
    MIN_COLS = 17 
    if stock_df.shape[1] < MIN_COLS:
        st.error(f"⚠️ Ошибка: В файле должно быть минимум {MIN_COLS} столбцов. Найдено: {stock_df.shape[1]}")
        return pd.DataFrame()
        
    stock_df.rename(columns={
        1: 'Name_Stock', 12: 'Store_Stock', 13: 'Qty_Stock', 16: 'Shelf_Stock' 
    }, inplace=True)
    
    stock_df_cleaned = stock_df[['Name_Stock', 'Store_Stock', 'Qty_Stock', 'Shelf_Stock']].copy()
    stock_df_cleaned.dropna(subset=['Name_Stock'], inplace=True)
    
    stock_names_list_lower = stock_df_cleaned['Name_Stock'].astype(str).str.strip().str.lower().unique().tolist()
    
    project_materials = data_df[['name', 'unit']].copy()
    project_materials.rename(columns={'name': 'Name_Project'}, inplace=True)
    project_materials['Name_Project_Lower'] = project_materials['Name_Project'].astype(str).str.strip().str.lower()
    
    project_materials['Name_Stock_Match'] = None
    project_materials['Match_Score'] = 0
    
    st.info(f"🔎 Запуск нечеткого сопоставления с порогом **{FUZZY_MATCH_THRESHOLD}%**...")
    
    matched_stock_data = {} 
    
    for index, row in project_materials.iterrows():
        project_name = row['Name_Project_Lower']
        
        best_match, score = find_best_match(project_name, stock_names_list_lower, FUZZY_MATCH_THRESHOLD)
        
        if score > 0:
            project_materials.at[index, 'Name_Stock_Match'] = best_match
            project_materials.at[index, 'Match_Score'] = score
            
            if best_match not in matched_stock_data:
                match_data = stock_df_cleaned[stock_df_cleaned['Name_Stock'].astype(str).str.strip().str.lower() == best_match]
                
                # Агрегирование 
                total_qty = match_data['Qty_Stock'].sum()
                all_stores = "; ".join(match_data['Store_Stock'].astype(str).unique().tolist())
                all_shelves = "; ".join(match_data['Shelf_Stock'].astype(str).unique().tolist())
                
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
    ).drop_duplicates(subset=['Name_Project']) 
    
    # 6. Очистка и форматирование результата
    result_df = final_df[[
        'Name_Project', 'unit', 'Qty_Stock_Agg', 'Store_Stock_Agg', 'Shelf_Stock_Agg', 'Match_Score'
    ]].copy()
    
    result_df.columns = ['Материал (План)', 'Ед. изм.', 'Количество (Склад)', 'Склады', 'Номера полок', 'Сходство (%)']
    
    result_df['Количество (Склад)'] = result_df['Количество (Склад)'].fillna(0).astype(float).round(2)
    result_df['Склады'] = result_df['Склады'].fillna('—')
    result_df['Номера полок'] = result_df['Номера полок'].fillna('—') 
    
    # Форматируем Сходство
    result_df['Сходство (%)'] = result_df['Сходство (%)'].apply(lambda x: f"{int(x)}%")
    
    st.success("🏁 Сопоставление завершено.")
    return result_df.sort_values(by=['Сходство (%)', 'Материал (План)'], ascending=[False, True])


# #######################################################
# 🖥️ ЛОГИКА ПРИЛОЖЕНИЯ (Streamlit UI)
# #######################################################

if not check_password():
    st.stop()

# Инициализируем БД (создаст таблицы, если они не существуют)
init_db()

# --- САЙДБАР (Обновленная логика бэкапа) ---
with st.sidebar:
    st.header("📂 Управление объектами")
    new_name = st.text_input("Имя нового объекта")
    if st.button("Добавить объект"):
        if new_name:
            if add_project(new_name):
                st.success("Создано!")
                st.rerun()
            else:
                st.error("Такое имя уже есть")
    
    st.divider()
    
    # Блок резервного копирования без работы с локальным файлом
    with st.expander("💾 Резервное копирование"):
        st.info("Внимание: Ваша база данных хранится на Supabase (PostgreSQL). Резервное копирование и восстановление осуществляется через панель управления Supabase.")
        if st.button("Открыть панель Supabase"):
            st.link_button("Supabase Dashboard", url="https://app.supabase.com/")

    st.divider()
    if st.button("Выйти из аккаунта"):
        logout()

# --- ОСНОВНОЕ ОКНО ---
st.title("🏗️Список всех объектов")

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
            # --- СЕКЦИЯ НАСТРОЕК / УДАЛЕНИЕ ---
            with st.expander("⚙️ Настройки / Удаление объекта"):
                # Блок Редактирования Названия
                st.write("**Редактирование названия**")
                new_pname = st.text_input("Новое название объекта", value=pname, key=f"edit_name_{pid}")
                if st.button("📝 Сохранить название", key=f"save_name_{pid}", type="secondary"):
                    if new_pname and new_pname != pname:
                        if update_project_name(pid, new_pname):
                            st.toast("Название обновлено!")
                            st.rerun()
                        else:
                            st.error("Ошибка: Такое название уже используется.")
                    else:
                        st.warning("Название не изменилось или пусто.")
                st.divider()

                # Блок Сброса и Удаления
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
                            st.rerun()
                        if col_no_d.button("Отмена", key=f"no_del_{pid}"):
                            st.session_state[confirm_delete_key] = False
                            st.rerun()
            
            # --- ДАННЫЕ (План и История) ---
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

                # --- ВВОД ПРИХОДА ---
                st.subheader("Ввод прихода")
                
                c1, c2, c3 = st.columns([3, 1, 2])
                
                opts = dict(zip(data_df['name'], data_df['id']))
                
                with c1:
                    s_name = st.selectbox("Материал", list(opts.keys()), key=f"sel_{pid}")
                    s_id = opts[s_name]
                    curr = data_df[data_df['id']==s_id].iloc[0]
                    st.caption(f"План: {curr['planned_qty']} {curr['unit']} | Факт: {curr['total']:.2f}")
                    
                input_key = f"num_{pid}"
                
                with c2:
                    val = st.number_input("Кол-во", min_value=0.0, step=1.0, key=input_key)
                
                with c3:
                    who = st.selectbox("Кто принял", WORKERS_LIST, key=f"who_{pid}")
                
                # --- СКРЫТИЕ ДОПОЛНИТЕЛЬНЫХ ПОЛЕЙ ПОД EXPANDER ---
                with st.expander("📝 Дополнительные данные (Магазин, Док. №, Прим.)"):
                    r2_c1, r2_c2 = st.columns(2)
                    
                    # Использование Session State для сохранения значений между reruns
                    store_key = f"store_{pid}"
                    doc_key = f"doc_{pid}"
                    note_key = f"note_{pid}"

                    if store_key not in st.session_state: st.session_state[store_key] = ""
                    if doc_key not in st.session_state: st.session_state[doc_key] = ""
                    if note_key not in st.session_state: st.session_state[note_key] = ""
                    
                    with r2_c1:
                        store_input = st.text_input("Магазин / Поставщик", key=store_key, value=st.session_state[store_key])

                    with r2_c2:
                        doc_input = st.text_input("Номер документа", key=doc_key, value=st.session_state[doc_key])
                        
                    note_input = st.text_area("Примечание", height=50, key=note_key, value=st.session_state[note_key])
                    
                # --- БЛОК КНОПОК УПРАВЛЕНИЯ ОПЕРАЦИЕЙ ---
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
                              args=(s_id, val, who, input_key, pid, st.session_state[store_key], st.session_state[doc_key], st.session_state[note_key]) 
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
                        st.rerun()
                
                # --- НОВЫЙ БЛОК: Сравнение с фактическими остатками (С СОХРАНЕНИЕМ ССЫЛКИ) ---
                st.divider()
                
                with st.expander("🔍 **Сравнение с фактическими остатками склада (по URL)**"):
                    st.info(f"Сравнение будет произведено с порогом сходства **{FUZZY_MATCH_THRESHOLD}%**.")
                    
                    col_url, col_btn = st.columns([4, 1])
                    
                    current_url = st.session_state.get(STOCK_URL_KEY, "")
                    
                    with col_url:
                        new_url = st.text_input(
                            "URL-ссылка на Excel/Google Таблицу", 
                            value=current_url, 
                            key=f"input_url_{pid}",
                            help="Вставьте ссылку Google Таблицы или прямую ссылку на Excel-файл. Нажмите 'Сохранить и сравнить', чтобы записать ее."
                        )
                        
                    with col_btn:
                        st.text(" ")
                        if st.button("💾 Сохранить и сравнить", key=f"save_compare_btn_{pid}", type="primary", use_container_width=True):
                            if new_url:
                                st.session_state[STOCK_URL_KEY] = new_url
                                st.session_state['trigger_compare'] = new_url
                                st.rerun()
                            else:
                                st.error("Поле ссылки не может быть пустым.")
                        
                    # КНОПКА ОБНОВЛЕНИЯ ПО СОХРАНЕННОЙ ССЫЛКЕ
                    if current_url:
                        st.markdown("---")
                        st.success(f"Текущая сохраненная ссылка: **{current_url[:60]}...**")
                        
                        if st.button("🔄 Обновить данные по сохраненной ссылке", key=f"refresh_compare_btn_{pid}", type="secondary", use_container_width=True):
                            st.session_state['trigger_compare'] = current_url
                            st.rerun()

                    # ЛОГИКА ОТОБРАЖЕНИЯ РЕЗУЛЬТАТОВ
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

                        # Выводим HTML-таблицу для форматированного текста
                        st.markdown(display_df.drop(columns=['id', 'Тип опер.']).to_html(escape=False, index=False), unsafe_allow_html=True)
                        
                        # Для скачивания используем исходный, неформатированный DataFrame
                        excel_data = to_excel(hist_df.drop(columns=['id']))
                        st.download_button(
                            label="📥 Скачать историю (Excel)",
                            data=excel_data,
                            file_name=f"История_{pname}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_{pid}"
                        )



