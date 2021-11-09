CREATE TABLE issues (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT NOT NULL,
                        repos INTEGER NOT NULL,
                        type TEXT NOT NULL,
                        start_date DATE NOT NULL,
                        end_date DATE NOT NULL,
                        auditors_count INTEGER NOT NULL,
                        specifications INTEGER NOT NULL,
                        published INTEGER NOT NULL,
                        findings_count INTEGER NOT NULL
);

CREATE TABLE auditors (
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          name TEXT NOT NULL
);

CREATE TABLE findings (
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          title TEXT,
                          severity TEXT NOT NULL,
                          auditor_id INTEGER NOT NULL,
                          issue_id INTEGER NOT NULL,

                          FOREIGN KEY (auditor_id) REFERENCES id,
                          FOREIGN KEY (issue_id) REFERENCES id
);

CREATE TABLE auditors_issues (
                                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                                   auditor_id INTEGER NOT NULL,
                                   issue_id INTEGER NOT NULL,

                                   FOREIGN KEY (auditor_id) REFERENCES id,
                                   FOREIGN KEY (issue_id) REFERENCES id
);