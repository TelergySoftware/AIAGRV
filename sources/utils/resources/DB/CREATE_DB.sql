CREATE TABLE auditors (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT NOT NULL
                        );

CREATE TABLE findings (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            title TEXT,
                            severity TEXT NOT NULL,
                            auditor_id INTEGER NOT NULL,

                            FOREIGN KEY (auditor_id) REFERENCES id
                        );