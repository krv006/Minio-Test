import pyodbc
import pandas as pd
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module='pandas')

server = '192.168.111.14'
database = 'CottonDb'
username = 'sa'
password = 'AX8wFfMQrR6b9qdhHt2eYS'
driver = '{SQL Server}'

conn_str = f"""
    DRIVER={driver};
    SERVER={server};
    DATABASE={database};
    UID={username};
    PWD={password};
"""
conn = pyodbc.connect(conn_str)

query = """
SELECT 
    F.[Id],
    F.[OwnerId],
    O.[Name] AS OrganizationName, 
    F.[FileName],
    F.[FilePath],
    F.[FileType],
    F.[TemplateId],
    F.[ParentId],
    F.[CreatedAt],
    F.[UpdatedAt],
    F.[IsDeleted]
FROM 
    [CottonDb].[dbo].[Files] F
JOIN 
    [CottonDb].[dbo].[Organizations] O ON F.OwnerId = O.Id;
"""

df = pd.read_sql(query, conn)
print(df)

conn.close()
