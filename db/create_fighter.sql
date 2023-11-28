CREATE TABLE IF NOT EXISTS fighter (
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  link VARCHAR(80) NOT NULL UNIQUE,
  name VARCHAR(40) NOT NULL,
  scraped INTEGER NOT NULL DEFAULT 0,
  success INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fighter_link ON fighter (link);
