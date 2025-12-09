import streamlit as st
import pandas as pd
from datetime import datetime
import json
import io
import requests
from thefuzz import fuzz
from thefuzz import process

# --- КОНСТАНТЫ ---
FUZZY_MATCH_THRESHOLD = 80
STOCK_URL_KEY = 'last_stock_url'
WORKERS_LIST = ["Выберите сотрудника...", "Хазбулат Р.", "Никулин Д.", "Волыкина Е.", "Ивонин К.", "Никонов Е.", "Губанов А.", "Яшковец В."]

# Базовая структура БД для первого запуска
EMPTY_DB_STRUCTURE = {
    'projects': pd.DataFrame(columns=['id', 'name']),
    'materials': pd.DataFrame(columns=['id', 'project_id', 'name', 'unit', 'planned_qty']),
    'shipments': pd.DataFrame(columns=['id', 'material_id', 'qty', 'user_name', 'arrival_date', 'store', 'doc_number', 'note', 'op_type'])
}

st.set_page_config(page_title="Склад обьекта", layout="wide")

# #######################################################
# 🔐 СЕРВИС: АУТЕНТИФИКАЦИЯ
# #######################################################

def check_password():
    """Проверяет пароль для доступа."""
    def password_entered():
        if st.session_state["password"] == st.secrets.get("password", "sclad_admin"):
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False
    
    if st.session_state.get("password_correct", False):
        return True

    st.text_input(
        "Пароль", type="password", on_change=password_entered, key="password"
    )
    if st.session_state.get("password_correct") is False:
        st.error("😕 Неверный пароль")
    return False

def logout():
    """Выход из аккаунта."""
    if "password_correct" in st.session_state:
        del st.session_state["password_correct"]
    st.rerun()

# #######################################################
# 💾 ФУНКЦИИ ХРАНЕНИЯ В SECRETS (CRUD)
# #######################################################

def enforce_types(df, table_name):
    """Приводит столбцы к нужным типам после загрузки из JSON."""
    if df.empty:
        return EMPTY_DB_STRUCTURE[table_name].copy()
    
    if table_name == 'projects':
        df['id'] = pd.to_numeric(df['id'], errors='coerce').fillna(0).astype(int)
    elif table_name == 'materials':
        df['id'] = pd.to_numeric(df['id'], errors='coerce').fillna(0).astype(int)
        df['project_id'] = pd.to_numeric(df['project_id'], errors='coerce').fillna(0).astype(int)
        df['planned_qty'] = pd.to_numeric(df['planned_qty'], errors='coerce').fillna(0.0)
    elif table_name == 'shipments':
        df['id'] = pd.to_numeric(df['id'], errors='coerce').fillna(0).astype(int)
        df['material_id'] = pd.to_numeric(df['material_id'], errors='coerce').fillna(0).astype(int)
        df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0.0)
    return df

@st.cache_data(ttl=5)
def load_db():
    """Загружает всю базу данных из secrets.toml (кэшируется на 5 секунд)."""
    try:
        db_json = st.secrets.storage.database_json
        
        # Если база данных пуста, инициализируем пустую структуру
        if db_json in ["{}", ""]:
            return EMPTY_DB_STRUCTURE
            
        db_data = json.loads(db_json)
        db = {}
        
        for key, df_json in db_data.items():
            df = pd.read_json(df_json, orient='split')
            db[key] = enforce_types(df, key)
            
        return db
        
    except Exception as e:
        # Если секрет не найден или невалиден, инициализируем пустую структуру
        if 'storage' not in st.secrets or 'database_json' not in st.secrets.storage:
             st.error("❌ Ошибка: Не найдена секция [storage] в secrets.toml. Проверьте файл.")
             st.stop()
        st.warning(f"Ошибка загрузки базы данных: {e}. Создается пустая структура.")
        return EMPTY_DB_STRUCTURE

def save_db(db):
    """Сохраняет всю базу данных обратно в secrets.toml."""
    try:
        # Для сохранения мы должны отключить кэш!
        st.cache_data.clear() 
        
        db_data = {}
        for key, df in db.items():
            # Преобразуем DataFrame в JSON-строку
            db_data[key] = df.to_json(orient='split', date_format='iso')

        # Сохраняем объединенный JSON в секреты
        st.secrets["storage"]["database_json"] = json.dumps(db_data)
        st.toast("💾 Данные сохранены в Streamlit Secrets.", icon="✅")
        return True
    except Exception as e:
        st.error(f"❌ Ошибка сохранения в Streamlit Secrets: {e}")
        return False

# #######################################################
# 🗃️ ФУНКЦИИ API (ОБНОВЛЕНО ДЛЯ IN-MEMORY DF)
# #######################################################

def get_projects():
    db = load_db()
    return db['projects'].sort_values(by='name')

def add_project(name):
    db = load_db()
    projects_df = db['projects']
    
    if name in projects_df['name'].tolist():
        return False
        
    new_id = projects_df['id'].max() + 1 if not projects_df.empty else 1
    new_row = pd.DataFrame([{'id': new_id, 'name': name}])
    
    db['projects'] = pd.concat([projects_df, new_row], ignore_index=True)
    
    return save_db(db)

def update_project_name(project_id, new_name):
    db = load_db()
    projects_df = db['projects']
    pid = int(project_id)
    
    if new_name in projects_df['name'].tolist():
        return False
        
    projects_df.loc[projects_df['id'] == pid, 'name'] = new_name
    db['projects'] = projects_df
    
    return save_db(db)

def delete_specific_project(project_id):
    db = load_db()
    pid = int(project_id)
    
    # 1. Удаление приходов, связанных с материалами этого проекта
    materials_to_delete = db['materials'][db['materials']['project_id'] == pid]['id'].tolist()
    db['shipments'] = db['shipments'][~db['shipments']['material_id'].isin(materials_to_delete)]
    
    # 2. Удаление материалов
    db['materials'] = db['materials'][db['materials']['project_id'] != pid]

    # 3. Удаление проекта
    db['projects'] = db['projects'][db['projects']['id'] != pid]
    
    save_db(db)

def clear_project_history(project_id):
    db = load_db()
    pid = int(project_id)
    
    materials_df = db['materials']
    
    # Идентификаторы материалов, которые НЕ относятся к этому проекту
    materials_to_keep = materials_df[materials_df['project_id'] != pid]['id'].tolist()
    
    # Оставляем только те приходы, которые не связаны с этим проектом
    db['shipments'] = db['shipments'][db['shipments']['material_id'].isin(materials_to_keep)]
    
    save_db(db)

def load_excel_final(project_id, df):
    db = load_db()
    pid = int(project_id)
    materials_df = db['materials']
    
    # 1. Удаляем старые материалы, связанные с этим проектом
    materials_df = materials_df[materials_df['project_id'] != pid]
    
    success = 0
    log = []
    insert_data = []
    
    # 2. Подготавливаем новые данные
    current_max_id = materials_df['id'].max() if not materials_df.empty else 0
    
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
                current_max_id += 1
                insert_data.append({
                    'id': current_max_id, 
                    'project_id': pid, 
                    'name': name, 
                    'unit': unit, 
                    'planned_qty': qty
                })
                success += 1
        except Exception as e:
            log.append(f"Ошибка строки {i}: {e}")
            
    # 3. Объединяем и сохраняем
    if insert_data:
        new_materials_df = pd.DataFrame(insert_data)
        db['materials'] = pd.concat([materials_df, new_materials_df], ignore_index=True)
        
        if save_db(db):
            return success, log
    
    return success, log

def add_shipment(material_id, qty, user, date, store, doc_number, note, op_type='Приход'):
    db = load_db()
    shipments_df = db['shipments']
    
    new_id = shipments_df['id'].max() + 1 if not shipments_df.empty else 1
    
    new_row = pd.DataFrame([{
        'id': new_id,
        'material_id': int(material_id),
        'qty': float(qty),
        'user_name': user,
        'arrival_date': date.strftime('%Y-%m-%d %H:%M:%S'),
        'store': store,
        'doc_number': doc_number,
        'note': note,
        'op_type': op_type
    }])
    
    db['shipments'] = pd.concat([shipments_df, new_row], ignore_index=True)
    
    if save_db(db):
        return new_id
    return None

def undo_shipment(shipment_id, current_user):
    db = load_db()
    shipments_df = db['shipments']
    
    original_data = shipments_df[shipments_df['id'] == shipment_id]
    
    if not original_data.empty:
        original_data = original_data.iloc[0]
        material_id = original_data['material_id']
        cancel_qty = -abs(original_data['qty'])
        
        # Записываем операцию "Отмена"
        new_id = shipments_df['id'].max() + 1 if not shipments_df.empty else 1
        
        new_row = pd.DataFrame([{
            'id': new_id,
            'material_id': material_id,
            'qty': cancel_qty,
            'user_name': current_user,
            'arrival_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'store': original_data['store'],
            'doc_number': original_data['doc_number'],
            'note': f"ОТМЕНА операции ID:{shipment_id}. Оригинальное Примечание: {original_data['note']}",
            'op_type': 'Отмена'
        }])
        
        db['shipments'] = pd.concat([shipments_df, new_row], ignore_index=True)
        
        if save_db(db):
            return True
    return False

def get_data(project_id):
    pid = int(project_id)
    db = load_db()
    
    materials_df = db['materials']
    shipments_df = db['shipments']
    
    project_materials = materials_df[materials_df['project_id'] == pid].copy()
    
    if project_materials.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # 1. Получение факта (total)
    if not shipments_df.empty:
        shipments_agg = shipments_df.groupby('material_id')['qty'].sum().reset_index()
        shipments_agg.rename(columns={'qty': 'total'}, inplace=True)
        
        full_df = pd.merge(project_materials, shipments_agg, left_on='id', right_on='material_id', how='left')
        full_df['total'] = full_df['total'].fillna(0)
    else:
        full_df = project_materials.copy()
        full_df['total'] = 0.0
    
    # 2. Расчет прогресса
    full_df['prog'] = full_df.apply(lambda x: x['total'] / x['planned_qty'] if x['planned_qty'] > 0 else 0, axis=1)

    # 3. История операций
    shipments_filtered = shipments_df[shipments_df['material_id'].isin(project_materials['id'])]
    
    if not shipments_filtered.empty:
        history_df = pd.merge(shipments_filtered, project_materials[['id', 'name', 'unit']], 
                             left_on='material_id', right_on='id', how='left', suffixes=('', '_mat'))
        
        history_df.rename(columns={
            'name': 'Материал', 
            'qty': 'Кол-во', 
            'op_type': 'Тип опер.', 
            'user_name': 'Кто', 
            'store': 'Магазин', 
            'doc_number': '№ Док.', 
            'note': 'Примечание', 
            'arrival_date': 'Дата',
            'unit': 'Ед. изм.' # Добавляем единицу измерения в историю
        }, inplace=True)
        
        history_df = history_df.sort_values(by='Дата', ascending=False)
        history_df = history_df[['id', 'Материал', 'Ед. изм.', 'Кол-во', 'Тип опер.', 'Кто', 'Магазин', '№ Док.', 'Примечание', 'Дата']]
    else:
        history_df = pd.DataFrame(columns=['id', 'Материал', 'Ед. изм.', 'Кол-во', 'Тип опер.', 'Кто', 'Магазин', '№ Док.', 'Примечание', 'Дата'])

    return full_df, history_df

def submit_entry_callback(material_id, qty, user, input_key, current_pid, store, doc_number, note):
    if user == "Выберите сотрудника..." or not user:
        st.toast("⚠️ Ошибка: Выберите фамилию сотрудника!", icon="❌")
        return

    if qty <= 0:
        st.toast("⚠️ Ошибка: Количество должно быть больше 0!", icon="❌")
        return

    try:
        latest_shipment_id = add_shipment(material_id, qty, user, datetime.now(), store, doc_number, note, op_type='Приход') 
        
        if latest_shipment_id:
            st.toast("✅ Данные успешно внесены!", icon="💾")
            st.session_state['last_shipment_id'] = latest_shipment_id
            st.session_state['last_shipment_pid'] = current_pid 
            st.session_state['current_user'] = user 
            
            st.session_state[input_key] = 0.0
            st.rerun() # Перезапуск для обновления данных
        else:
            st.toast("Ошибка записи в Streamlit Secrets.", icon="🔥")
        
    except Exception as e:
        st.toast(f"Ошибка записи: {e}", icon="🔥")


# #######################################################
# 🛠️ ФУНКЦИИ УТИЛИТ
# #######################################################

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

def compare_with_stock_excel(file_source, data_df):
    # ... (логика сравнения с Excel/URL остается без изменений, так как не зависит от БД)
    stock_df = pd.DataFrame()
    
    if isinstance(file_source, str):
        original_url = file_source.strip()
        
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
                
                total_qty = match_data['Qty_Stock'].sum()
                all_stores = "; ".join(match_data['Store_Stock'].astype(str).unique().tolist())
                all_shelves = "; ".join(match_data['Shelf_Stock'].astype(str).unique().tolist())
                
                matched_stock_data[best_match] = {
                    'Qty_Stock_Agg': total_qty,
                    'Store_Stock_Agg': all_stores,
                    'Shelf_Stock_Agg': all_shelves
                }

    matched_df = pd.DataFrame.from_dict(matched_stock_data, orient='index').reset_index()
    matched_df.rename(columns={'index': 'Name_Stock_Match'}, inplace=True)
    
    final_df = pd.merge(
        project_materials, 
        matched_df, 
        on='Name_Stock_Match', 
        how='left'
    ).drop_duplicates(subset=['Name_Project']) 
    
    result_df = final_df[[
        'Name_Project', 'unit', 'Qty_Stock_Agg', 'Store_Stock_Agg', 'Shelf_Stock_Agg', 'Match_Score'
    ]].copy()
    
    result_df.columns = ['Материал (План)', 'Ед. изм.', 'Количество (Склад)', 'Склады', 'Номера полок', 'Сходство (%)']
    
    result_df['Количество (Склад)'] = result_df['Количество (Склад)'].fillna(0).astype(float).round(2)
    result_df['Склады'] = result_df['Склады'].fillna('—')
    result_df['Номера полок'] = result_df['Номера полок'].fillna('—') 
    
    result_df['Сходство (%)'] = result_df['Сходство (%)'].apply(lambda x: f"{int(x)}%")
    
    st.success("🏁 Сопоставление завершено.")
    return result_df.sort_values(by=['Сходство (%)', 'Материал (План)'], ascending=[False, True])


# #######################################################
# 🖥️ ЛОГИКА ПРИЛОЖЕНИЯ (Streamlit UI)
# #######################################################

if not check_password():
    st.stop()

# --- САЙДБАР ---
with st.sidebar:
    st.header("📂 Управление объектами")
    new_name = st.text_input("Имя нового объекта")
    if st.button("Добавить объект"):
        if new_name:
            if add_project(new_name):
                st.toast("Объект создан!")
                st.rerun()
            else:
                st.error("Такое имя уже есть")
    
    st.divider()
    
    with st.expander("💾 Резервное копирование"):
        st.info("Данные хранятся в файле `.streamlit/secrets.toml`.")
        st.warning("Для резервного копирования сохраните содержимое секции `[storage]` из этого файла.")

    st.divider()
    if st.button("Выйти из аккаунта"):
        logout()

# --- ОСНОВНОЕ ОКНО ---
st.title("🏗️Список всех объектов")

# Проверка и загрузка данных
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
                    st.write("**Сброс данных** (только история приходов)")
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
                    who = st.selectbox("Кто принял", WORKERS_LIST, key=f"who_{pid}", value=st.session_state.get('current_user', WORKERS_LIST[0]))
                
                # --- СКРЫТИЕ ДОПОЛНИТЕЛЬНЫХ ПОЛЕЙ ПОД EXPANDER ---
                with st.expander("📝 Дополнительные данные (Магазин, Док. №, Прим.)"):
                    r2_c1, r2_c2 = st.columns(2)
                    
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
                            help="Вставьте ссылку Google Таблицы (с экспортом в xlsx) или прямую ссылку на Excel-файл."
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
                        # Форматируем столбец "Кол-во"
                        display_df['Кол-во'] = display_df.apply(format_qty_and_type, axis=1)

                        # Отображаем как HTML для цвета
                        st.markdown(display_df.drop(columns=['id', 'Тип опер.']).to_html(escape=False, index=False), unsafe_allow_html=True)
                        
                        # Скачивание (используем исходный DataFrame без HTML-разметки)
                        excel_data = to_excel(hist_df.drop(columns=['id']))
                        st.download_button(
                            label="📥 Скачать историю (Excel)",
                            data=excel_data,
                            file_name=f"История_{pname}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"dl_{pid}"
                        )


