import pandas as pd
import requests
from urllib.parse import urlparse
import re
import os
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog

def is_valid_url(url):
    if pd.isna(url):
        return False
    if re.search(r"\s", str(url)):
        return False
    
    parsed = urlparse(str(url))
    
    if parsed.scheme and parsed.scheme not in ('http', 'https'):
        return False
    
    domain = parsed.netloc if parsed.netloc else parsed.path.split('/')[0]
    domain_pattern = r'^[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,}$'
    
    return re.match(domain_pattern, domain) is not None

def check_website(url, timeout=5):
    if pd.isna(url) or not is_valid_url(url):
        return "Formato inválido"
    
    url = str(url).strip()
    
    try:
        if not url.startswith(('http://', 'https://')):
            prefixed_url = f'https://{url}'
            response = requests.head(prefixed_url, timeout=timeout, allow_redirects=True)
        else:
            response = requests.head(url, timeout=timeout, allow_redirects=True)
        
        if response.status_code < 400:
            return f"Accesible (HTTP {response.status_code})"
        
        return f"Error HTTP {response.status_code}"
    
    except requests.exceptions.SSLError:
        try:
            insecure_url = url.replace('https://', 'http://') if url.startswith('https://') else f'http://{url}'
            response = requests.head(insecure_url, timeout=timeout, allow_redirects=True)
            return f"Accesible con HTTP (no seguro) - HTTP {response.status_code}"
        except:
            return "Inaccesible"
    
    except requests.exceptions.RequestException as e:
        return f"Error: {str(e)}"
    
    except Exception as e:
        return f"Error inesperado: {str(e)}"

def main():
    root = tk.Tk()
    root.withdraw()
    
    # Seleccionar archivo Excel
    file_path = filedialog.askopenfilename(
        title="Seleccionar archivo Excel",
        filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
    )
    
    if not file_path:
        messagebox.showinfo("Información", "No se seleccionó ningún archivo")
        return
    
    # Pedir nombre de hoja (opcional)
    sheet_name = simpledialog.askstring(
        "Configuración",
        "Nombre de la hoja (deja vacío para la primera hoja):",
        parent=root
    )
    
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name if sheet_name else 0)
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo leer el archivo:\n{str(e)}")
        return
    
    # Buscar automáticamente columna URL
    target_column = "URL"
    if target_column not in df.columns:
        messagebox.showerror("Error", f"No se encontró la columna '{target_column}'")
        return
    
    # Verificar URLs
    messagebox.showinfo("Proceso", "Verificando URLs... Esto puede tomar unos momentos")
    df['Estado_URL'] = df[target_column].apply(check_website)
    
    # Guardar resultados
    output_path = os.path.splitext(file_path)[0] + "_verificado.xlsx"
    try:
        df.to_excel(output_path, index=False)
        messagebox.showinfo(
            "Éxito",
            f"Proceso completado correctamente!\nArchivo guardado en:\n{output_path}"
        )
    except Exception as e:
        messagebox.showerror("Error", f"No se pudo guardar el archivo:\n{str(e)}")

if __name__ == "__main__":
    main()