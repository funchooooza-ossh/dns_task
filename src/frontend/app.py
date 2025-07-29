import pandas as pd
import requests
import streamlit as st

st.set_page_config(layout="wide")
st.title("Оптимальное распределение товаров")

# Базовые настройки
api_url = st.text_input("URL API", value="http://localhost:8000/distribution")

st.markdown("### Настройки источников")
col1, col2, col3 = st.columns(3)
with col1:
	schema = st.text_input("Схема", "logistics")
	rc_table = st.text_input("Таблица РЦ", "rc_product_history")
	branch_table = st.text_input("Таблица филиалов", "branch_product_history")
with col2:
	needs_table = st.text_input("Таблица потребностей", "needs")
	min_table = st.text_input("min_shipment", "min_shipment")
	volume_table = st.text_input("products_vol", "products_vol")
with col3:
	limit_table = st.text_input("storage_limits", "storage_limits")
	product_table = st.text_input("products", "products")

st.markdown("### Фильтры распределения")
col4, col5 = st.columns(2)
with col4:
	respect_volume = st.checkbox("Учитывать объем", value=True)
	limit = st.slider("Лимит строк", 10, 500, 100)
with col5:
	min_demand = st.number_input("Минимальный спрос", min_value=0.0, value=0.0)

if st.button("Запустить расчет"):
	with st.spinner("Считаем..."):
		params = {
			"schema": schema,
			"rc_table": rc_table,
			"branch_table": branch_table,
			"needs_table": needs_table,
			"min_table": min_table,
			"volume_table": volume_table,
			"limit_table": limit_table,
			"product_table": product_table,
			"respect_volume": respect_volume,
			"limit": limit,
			"min_demand": min_demand,
		}

		try:
			response = requests.get(api_url, params=params)  # noqa: S113
			response.raise_for_status()
			data = response.json()
			if not data:
				st.warning("Нет данных для отображения.")
			else:
				df = pd.DataFrame(data)

				st.subheader("Результат распределения")
				st.dataframe(df, use_container_width=True)

				# График распределения по товарам
				st.markdown("#### Распределение по товарам")
				st.bar_chart(df.groupby("product_id")["qty"].sum())

				# Топ-5 филиалов по объему
				st.markdown("#### Топ-5 филиалов по отгрузке")
				df["qty"] = pd.to_numeric(
					df["qty"], errors="coerce"
				)  # преобразуем qty в число
				top_branches = df.groupby("branch_id")["qty"].sum().nlargest(5)
				st.bar_chart(top_branches)

		except Exception as e:
			st.error(f"Ошибка при запросе API: {e}")
