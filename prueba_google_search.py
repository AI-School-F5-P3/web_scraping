import googlesearch

def search_company_website(company_name):
    query = f"{company_name} sitio web oficial"
    results = googlesearch.search(query, num_results=3)
    return list(results)

# Buscar dominio real de la empresa
company_name = "ARISCO SA en terrassa"
real_urls = search_company_website(company_name)
print(real_urls)