INSERT INTO main.supplier
        VALUES (3, CAST(nullif (CAST(NULL AS VARCHAR), CASE WHEN 56 IS NOT NULL THEN
                        CAST(NULL AS VARCHAR)
                    ELSE
                        CAST(NULL AS VARCHAR)
                    END) AS VARCHAR), CAST(NULL AS VARCHAR), 11, CAST(NULL AS VARCHAR), DEFAULT, CAST(NULL AS VARCHAR))
