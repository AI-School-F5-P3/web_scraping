import requests

def get_company_by_irus(irus):
    url = f"https://opendata.registradores.org/api/sociedades/byIrus/{irus}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return {
                'denominacion': data.get('denominacionSocial'),
                'nif': data.get('cif'),
                'forma_juridica': data.get('formaSocial')
            }
        else:
            return f"Error: Status code {response.status_code}"
    except Exception as e:
        return f"Error en la consulta: {str(e)}"

# Ejemplo de uso
irus = "1000003295252"  # IRUS de CBD EUSKADI, S.L.
resultado = get_company_by_irus(irus)
print(resultado)