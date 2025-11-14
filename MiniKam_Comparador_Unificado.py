import streamlit as st
import pandas as pd
import re
from urllib.parse import urlparse
from io import BytesIO
from datetime import datetime

st.set_page_config(page_title="MiniKAM Comparador Unificado", layout="wide")
st.title("📊 MiniKAM — Comparador Automático con Enriquecimiento (Enriquecer primero)")

# -------------------------
# Utilidades
# -------------------------
def clean_product_name(name: str) -> str:
    if not isinstance(name, str): return ""
    s = name.lower()
    s = re.sub(r'\(.*?\)|\[.*?\]', ' ', s)
    s = re.sub(r'[-/]', ' ', s)
    s = re.sub(r'\b\d+(\.\d+)?\s?(g|gr|kg|ml|l|pz|pza|pieza|pk|pack|paquete|caja|oz)\b', ' ', s, flags=re.IGNORECASE)
    s = re.sub(r'\b\d+(g|gr|kg|ml|l|oz)\b', ' ', s, flags=re.IGNORECASE)
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def extraer_codigo_mas_largo(texto: str, min_len: int = 4, max_len: int = 20) -> str:
    if not isinstance(texto, str): return ""
    pattern = r"\d{" + str(min_len) + "," + str(max_len) + r"}"
    matches = re.findall(pattern, texto)
    if matches: return max(matches, key=len)
    fallback = re.findall(r"\d{4,}", texto)
    return max(fallback, key=len) if fallback else ""

def detectar_competidor_desde_url(url: str) -> str:
    try:
        if not isinstance(url, str) or url.strip() == "":
            return ""
        host = urlparse(url).netloc.lower().split(':')[0]
        parts = [p for p in host.split('.') if p and p != 'www']
        return parts[0] if parts else host
    except:
        return ""

# -------------------------
# Reordenamiento robusto
# -------------------------
def norm(s):
    if s is None:
        return ""
    s = str(s).lower()
    s = re.sub(r'[^a-z0-9]+', '', s)
    return s

def pick_column(actual_cols, target_variants):
    norm_map = {norm(c): c for c in actual_cols}
    for v in target_variants:
        key = norm(v)
        if key in norm_map:
            return norm_map[key]
    for v in target_variants:
        key = norm(v)
        for k, real in norm_map.items():
            if key in k or k in key:
                return real
    return None

def reorder_df_to_exact_columns(df, desired_order):
    actual = df.columns.tolist()
    selected_cols = []
    used_actual = set()

    for desired in desired_order:
        variants = [desired, desired.lower(), desired.replace("  "," "), desired.replace(" ", ""), desired.replace(" ", "_")]
        found = pick_column(actual, variants)
        if found and found not in used_actual:
            selected_cols.append(found)
            used_actual.add(found)
        else:
            # crear columna vacía con el nombre exacto pedido (garantiza posición en Excel)
            df[desired] = pd.NA
            selected_cols.append(desired)

    # Añadir columnas restantes al final (que no están en la lista deseada)
    for c in actual:
        if c not in used_actual and c not in selected_cols:
            selected_cols.append(c)

    return df.loc[:, selected_cols]

# -------------------------
# Consolidación y limpieza robusta
# -------------------------
def consolidate_duplicate_columns(df, canonical_names_map=None):
    norm_map = {}
    order = list(df.columns)
    for col in order:
        key = re.sub(r'\s+', ' ', str(col)).strip().lower()
        key = re.sub(r'[_\s]+', ' ', key)
        norm_map.setdefault(key, []).append(col)

    for key, cols in norm_map.items():
        if len(cols) <= 1:
            continue

        counts = {}
        for c in cols:
            try:
                ser = df[c]
                non_blank = (~ser.isna()) & (ser.astype(str).str.strip() != "")
                counts[c] = int(non_blank.sum())
            except Exception:
                counts[c] = 0

        winner = max(cols, key=lambda x: (counts.get(x, 0), -cols.index(x)))

        canonical_display = None
        if canonical_names_map:
            if key in canonical_names_map:
                canonical_display = canonical_names_map[key]

        if not canonical_display:
            canonical_display = re.sub(r'\s+', ' ', str(winner)).strip()

        if canonical_display in df.columns and canonical_display != winner:
            try:
                existing_nonblank = ((~df[canonical_display].isna()) & (df[canonical_display].astype(str).str.strip() != "")).any()
            except Exception:
                existing_nonblank = False
            if not existing_nonblank:
                df.rename(columns={canonical_display: canonical_display + "_old_empty"}, inplace=True)
            else:
                winner = canonical_display

        if winner != canonical_display:
            df.rename(columns={winner: canonical_display}, inplace=True)
            winner = canonical_display

        losers = [c for c in cols if c != winner and c in df.columns]
        if losers:
            df.drop(columns=losers, inplace=True)

    return df

def canonicalize_and_drop(df):
    df.columns = [re.sub(r'\s+', ' ', str(c)).strip() for c in df.columns]

    rename_map = {
        "Costo de  oferta": "Costo de oferta",
        "Valor del anaquel  por SKU": "Valor del anaquel por SKU",
        "URL Imagen": "imagen_url",
        "URL_Imagen": "imagen_url",
        "URLImagen": "imagen_url",
        "URL Imagen_0": "imagen_url",
        "Valor del anaquel por SKU_1": "Valor del anaquel por SKU",
        "Precio de la Competencia_1": "Precio de la Competencia"
    }
    existing_map = {k: v for k, v in rename_map.items() if k in df.columns}
    if existing_map:
        df.rename(columns=existing_map, inplace=True)

    names_to_check = [
        "Costo de oferta", "precio_comp", "Precio de la Competencia", "imagen_url",
        "Valor del anaquel por SKU", "URL Imagen", "URL_Imagen", "URLImagen", "URL Imagen_0"
    ]
    for name in names_to_check:
        if name in df.columns:
            ser = df[name]
            try:
                all_na = ser.isna().all()
                all_blank = (ser.astype(str).str.strip() == "").all()
            except Exception:
                all_na = False
                all_blank = False
            if all_na or all_blank:
                df.drop(columns=[name], inplace=True)

    positions_to_drop = [16, 17, 33, 38]  # fallback P,Q,AG,AL
    for pos in sorted(positions_to_drop, reverse=True):
        idx = pos - 1
        if 0 <= idx < len(df.columns):
            colname = df.columns[idx]
            try:
                ser = df[colname]
                all_na = ser.isna().all()
                all_blank = (ser.astype(str).str.strip() == "").all()
            except Exception:
                all_na = False
                all_blank = False
            if all_na or all_blank:
                df.drop(columns=[colname], inplace=True)

    return df

# canonical names map sugerido
canonical_map = {
    "costo de oferta": "Costo de oferta",
    "costo de  oferta": "Costo de oferta",
    "precio_comp": "precio_comp",
    "precio comp": "precio_comp",
    "precio de la competencia": "Precio de la Competencia",
    "valor del anaquel por sku": "Valor del anaquel por SKU",
    "valor del anaquel  por sku": "Valor del anaquel por SKU",
    "imagen_url": "imagen_url",
    "url imagen": "imagen_url",
    "url_imagen": "imagen_url",
    "url imagen_0": "imagen_url"
}

# -------------------------
# Mover/insertar columna a posición Excel (1-based)
# -------------------------
def move_column_to_position(df, col_name, pos_1based):
    if not isinstance(pos_1based, int) or pos_1based < 1:
        raise ValueError("pos_1based debe ser entero >= 1")

    if col_name in df.columns:
        series = df.pop(col_name)
    else:
        series = pd.Series([pd.NA] * len(df), name=col_name)

    insert_idx = pos_1based - 1
    if insert_idx < 0:
        insert_idx = 0
    if insert_idx > len(df.columns):
        insert_idx = len(df.columns)

    df.insert(insert_idx, col_name, series)
    return df

# -------------------------
# Uploads
# -------------------------
col1, col2 = st.columns(2)
with col1:
    archivo_datos = st.file_uploader("📥 Sube archivo MiniKam (.xlsx)", type=["xlsx"])
with col2:
    archivo_comp = st.file_uploader("📥 Sube archivo Web Scraper (crudo .xlsx o .csv)", type=["xlsx", "csv"])

if not archivo_datos or not archivo_comp:
    st.info("Sube ambos archivos para continuar.")
    st.stop()

# -------------------------
# Leer y normalizar MiniKam
# -------------------------
try:
    df_datos = pd.read_excel(archivo_datos)
except Exception as e:
    st.error(f"❌ Error leyendo MiniKam: {e}")
    st.stop()

df_datos.columns = df_datos.columns.str.strip()
col_desc = "Descripción del producto"
if col_desc not in df_datos.columns:
    st.error("❌ No se encontró la columna 'Descripción del producto' en MiniKam. Verifica el archivo.")
    st.write("Columnas detectadas:", df_datos.columns.tolist())
    st.stop()

df_datos["nombre_clean"] = df_datos[col_desc].astype(str).apply(clean_product_name)

# -------------------------
# Enriquecer archivo de competencia (PRIMERO)
# -------------------------
try:
    if archivo_comp.name.lower().endswith('.xlsx'):
        df_comp = pd.read_excel(archivo_comp)
    else:
        df_comp = pd.read_csv(archivo_comp)
except Exception as e:
    st.error(f"❌ Error leyendo archivo de competencia: {e}")
    st.stop()

df_comp = df_comp.copy()
df_comp.columns = df_comp.columns.str.strip()
cols = df_comp.columns.tolist()

col_url = next((c for c in cols if 'url' in c.lower() and ('producto' in c.lower() or 'product' in c.lower())), None)
if not col_url:
    col_url = next((c for c in cols if 'url' in c.lower()), None)

col_img = next((c for c in cols if 'imagen' in c.lower() or 'img' in c.lower()), None)
col_nombre = next((c for c in cols if 'nombre' in c.lower() or 'producto' in c.lower() or 'descripcion' in c.lower()), None)
col_precio = next((c for c in cols if 'precio' in c.lower() or 'mxn' in c.lower()), None)

if col_url:
    df_comp["codigo_desde_url"] = df_comp[col_url].astype(str).fillna("").apply(extraer_codigo_mas_largo)
    df_comp["competidor"] = df_comp[col_url].astype(str).fillna("").apply(detectar_competidor_desde_url)
else:
    df_comp["codigo_desde_url"] = ""
    df_comp["competidor"] = ""

if col_img:
    df_comp["codigo_desde_imagen"] = df_comp[col_img].astype(str).fillna("").apply(extraer_codigo_mas_largo)
else:
    df_comp["codigo_desde_imagen"] = ""

df_comp["codigo_extraido"] = df_comp["codigo_desde_imagen"].where(df_comp["codigo_desde_imagen"] != "", df_comp["codigo_desde_url"])
df_comp["nombre_clean"] = df_comp[col_nombre].astype(str).fillna("").apply(clean_product_name) if col_nombre else ""
if col_precio:
    df_comp["precio_comp"] = pd.to_numeric(df_comp[col_precio].astype(str).str.replace('[^0-9.,]', '', regex=True).str.replace(',','.'), errors='coerce')
else:
    df_comp["precio_comp"] = pd.NA

df_comp["imagen_url"] = df_comp[col_img] if col_img else ""

st.success(f"Enriquecimiento completado — filas competencia: {len(df_comp)}")
with st.expander("Vista previa (competencia enriquecida)"):
    preview_cols = [c for c in ["competidor","codigo_extraido","nombre_clean","precio_comp","imagen_url"] if c in df_comp.columns]
    st.dataframe(df_comp[preview_cols].head(50))

# -------------------------
# Normalizar claves para merge (convertir a str limpio)
# -------------------------
if "Codigo" in df_datos.columns:
    df_datos["Codigo"] = df_datos["Codigo"].fillna("").astype(str).str.strip()
    df_datos["Codigo"] = df_datos["Codigo"].str.replace(r'\.0+$', '', regex=True).str.replace(r'\s+', '', regex=True)
else:
    df_datos["Codigo"] = ""

if "codigo_extraido" in df_comp.columns:
    df_comp["codigo_extraido"] = df_comp["codigo_extraido"].fillna("").astype(str).str.strip()
    df_comp["codigo_extraido"] = df_comp["codigo_extraido"].str.replace(r'\.0+$', '', regex=True).str.replace(r'\s+', '', regex=True)
else:
    df_comp["codigo_extraido"] = ""

# -------------------------
# Obtener lista de competidores desde df_comp enriquecido
# -------------------------
competidores = [c for c in df_comp["competidor"].dropna().unique().tolist() if c and str(c).strip() != ""]
if len(competidores) == 0:
    alt_cols = [c for c in df_comp.columns if any(k in c.lower() for k in ["tienda","source","seller","vendor"])]
    if alt_cols:
        df_comp["competidor"] = df_comp[alt_cols[0]].astype(str).fillna("").apply(lambda x: clean_product_name(x).split()[0] if isinstance(x, str) else "")
        competidores = [c for c in df_comp["competidor"].dropna().unique().tolist() if c and str(c).strip() != ""]

# -------------------------
# Validar columnas necesarias en MiniKam antes de cálculos
# -------------------------
required_cols = ["Codigo","Costo","Margen objetivo","Oferta programada","Inventario",
                 "Producto Altura cm","Producto Ancho cm","Frentes en el anaquel",
                 "charolas para la sub categoria","Ancho de la charola cm","Profundo de la charola"]
missing = [c for c in required_cols if c not in df_datos.columns]
if missing:
    st.error(f"❌ Faltan columnas necesarias en MiniKam: {missing}")
    st.write("Columnas detectadas:", df_datos.columns.tolist())
    st.stop()

# -------------------------
# Cálculos base en MiniKam
# -------------------------
try:
    df_datos["Margen Obtenido"] = (df_datos["Oferta programada"] - df_datos["Costo"]) / df_datos["Oferta programada"]
    df_datos["Precio Vta Sugerido"] = df_datos["Costo"] / (1 - df_datos["Margen objetivo"])
    df_datos["Costo de oferta"] = df_datos["Precio Vta Sugerido"] - df_datos["Oferta programada"]
    df_datos["Costo de inventario"] = df_datos["Costo"] * df_datos["Inventario"]
    df_datos["Espacio lineal en % por producto"] = (df_datos["Producto Ancho cm"] * df_datos["Frentes en el anaquel"]) / (df_datos["charolas para la sub categoria"] * df_datos["Ancho de la charola cm"])
    df_datos["Inventario en el anaquel"] = ((df_datos["Profundo de la charola"] // df_datos["Producto Altura cm"]) * df_datos["Frentes en el anaquel"]).fillna(0).astype(int)
    df_datos["Valor del anaquel por SKU"] = df_datos["Inventario en el anaquel"] * df_datos["Precio Vta Sugerido"]
except Exception as e:
    st.error(f"❌ Error en cálculos base: {e}")
    st.stop()

# -------------------------
# Orden deseado (exacto)
# -------------------------
desired_order = [
    "Marca", "Proveedor", "Categoria", "Sub categoria", "Segmento 1", "Codigo",
    "Material", "Descripción del producto", "Costo", "Margen objetivo", "Margen Obtenido",
    "Precio Vta Sugerido", "Oferta programada", "Costo de oferta", "precio_comp",
    "Costo de  oferta", "Precio de la Competencia", "Competividad", "Vantas Mensules  en piezas",
    "Inventario", "Disas de inventario", "Costo de inventario", "Producto Altura cm",
    "Producto Ancho cm", "Producto Profundo cm", "Frentes en el anaquel",
    "charolas para la sub categoria", "Ancho de la charola cm", "Valor del anaquel por SKU",
    "Profundo de la charola", "Espacio lineal en % por producto", "Inventario en el anaquel",
    "Valor del anaquel  por SKU", "nombre_clean", "codigo_extraido", "imagen_url", "Estado"
]

# -------------------------
# Cruce y generación de Excel
# -------------------------
buffer = BytesIO()
hojas_generadas = 0
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
file_name = f"MiniKam_Comparativo_{timestamp}.xlsx"

with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
    pd.DataFrame({"Info": ["Archivo generado por MiniKAM - inicio"]}).to_excel(writer, sheet_name="Inicio", index=False)
    hojas_generadas += 1

    for comp in sorted(competidores):
        if not comp or str(comp).strip() == "":
            continue
        df_competidor = df_comp[df_comp["competidor"] == comp]
        if df_competidor.empty:
            continue

        df_codigo = pd.merge(
            df_datos,
            df_competidor[["codigo_extraido", "precio_comp", "imagen_url"]].drop_duplicates(),
            left_on="Codigo", right_on="codigo_extraido", how="left"
        )
        df_nombre = pd.merge(
            df_datos,
            df_competidor[["nombre_clean", "precio_comp", "imagen_url"]].drop_duplicates(),
            on="nombre_clean", how="left"
        )

        df_final = df_codigo.copy()
        df_final["precio_comp"] = df_codigo["precio_comp"].combine_first(df_nombre["precio_comp"])
        df_final["imagen_url"] = df_codigo["imagen_url"].combine_first(df_nombre["imagen_url"])
        df_final["Estado"] = df_final["precio_comp"].apply(lambda x: "No lo tiene el competidor" if pd.isna(x) else "Coincide")
        df_final["Competividad"] = df_final.apply(
            lambda r: (r["Oferta programada"] - r["precio_comp"]) / r["Oferta programada"]
            if pd.notna(r["precio_comp"]) and r["Oferta programada"] not in (0, None) else pd.NA,
            axis=1
        )

        try:
            df_final_reordered = reorder_df_to_exact_columns(df_final.copy(), desired_order)
        except Exception as e:
            st.warning(f"Advertencia al reordenar columnas para {comp}: {e}. Se usará el orden por defecto.")
            df_final_reordered = df_final

        # Consolidar duplicados y limpiar
        df_final_reordered = consolidate_duplicate_columns(df_final_reordered, canonical_map)
        df_final_reordered = canonicalize_and_drop(df_final_reordered)

        # mover 'Valor del anaquel por SKU' a columna U (pos 21)
        df_final_reordered = move_column_to_position(df_final_reordered, "Valor del anaquel por SKU", 21)

        if not df_final_reordered.empty:
            safe_name = (str(comp) or "competidor")[:31]
            sheet_name = safe_name
            suffix = 1
            while sheet_name in writer.book.sheetnames:
                sheet_name = f"{safe_name[:28]}_{suffix}"
                suffix += 1
            df_final_reordered.to_excel(writer, sheet_name=sheet_name, index=False)
            hojas_generadas += 1

    # MiniKam Original
    try:
        if df_datos.shape[0] > 0 and df_datos.shape[1] > 0:
            base_name = "MiniKam Original"
            name_try = base_name
            i = 1
            while name_try in writer.book.sheetnames:
                name_try = f"{base_name}_{i}"
                i += 1

            try:
                df_datos_reordered = reorder_df_to_exact_columns(df_datos.copy(), desired_order)
                df_datos_reordered = consolidate_duplicate_columns(df_datos_reordered, canonical_map)
                df_datos_reordered = canonicalize_and_drop(df_datos_reordered)

                # mover 'Valor del anaquel por SKU' a columna U (pos 21) en MiniKam Original
                df_datos_reordered = move_column_to_position(df_datos_reordered, "Valor del anaquel por SKU", 21)

                df_datos_reordered.to_excel(writer, sheet_name=name_try, index=False)
                hojas_generadas += 1
            except Exception:
                df_datos.to_excel(writer, sheet_name=name_try, index=False)
                hojas_generadas += 1
    except Exception:
        pass

    if hojas_generadas <= 1:
        diag = {
            "Mensaje": ["No se generaron hojas de competidor con datos. Revisa archivos subidos."],
            "Productos_MiniKam": [len(df_datos)],
            "Filas_Competencia": [len(df_comp)],
            "Competidores_detectados": [len([c for c in df_comp['competidor'].unique() if c and str(c).strip() != ''])]
        }
        if "NoData" in writer.book.sheetnames:
            pd.DataFrame(diag).to_excel(writer, sheet_name="NoData_2", index=False)
        else:
            pd.DataFrame(diag).to_excel(writer, sheet_name="NoData", index=False)

buffer.seek(0)
st.success("✅ Archivo comparativo generado (o hoja NoData si no hubo coincidencias).")
st.download_button("📥 Descargar archivo Excel", buffer, file_name=file_name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.subheader("Resumen rápido")
st.write(f"- Productos en MiniKam: {len(df_datos)}")
st.write(f"- Filas en competencia (enriquecidas): {len(df_comp)}")
st.write(f"- Competidores detectados: {len([c for c in df_comp['competidor'].unique() if c and str(c).strip() != ''])}")
if len([c for c in df_comp['competidor'].unique() if c and str(c).strip() != '']) > 0:
    st.table(pd.Series(df_comp['competidor'].dropna()).value_counts().rename_axis("Competidor").reset_index(name="count").head(20))
