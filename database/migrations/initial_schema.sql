-- database/migrations/initial_schema.sql
CREATE DATABASE IF NOT EXISTS webscraping_db
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE webscraping_db;

CREATE TABLE IF NOT EXISTS empresas (
    id INT AUTO_INCREMENT PRIMARY KEY,
    codigo_infotel VARCHAR(20) UNIQUE NOT NULL,
    nif VARCHAR(15) NOT NULL,
    razon_social VARCHAR(255) NOT NULL,
    direccion TEXT,
    codigo_postal VARCHAR(5),
    poblacion VARCHAR(100),
    provincia VARCHAR(50),
    website VARCHAR(500),
    url_valid BOOLEAN DEFAULT FALSE,
    telefonos TEXT,
    redes_sociales TEXT,
    ecommerce BOOLEAN DEFAULT FALSE,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    confidence_score INT,
    INDEX idx_codigo_infotel (codigo_infotel),
    INDEX idx_nif (nif),
    INDEX idx_provincia (provincia),
    INDEX idx_codigo_postal (codigo_postal),
    INDEX idx_ecommerce (ecommerce)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS scraping_logs (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    empresa_id INT NOT NULL,
    status VARCHAR(50),
    intentos INT DEFAULT 0,
    detalle_error TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_empresa (empresa_id),
    INDEX idx_timestamp (timestamp),
    FOREIGN KEY (empresa_id) REFERENCES empresas(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;