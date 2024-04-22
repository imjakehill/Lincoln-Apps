Txt Validator 8.2

To configure the database connection open 'config_connection.txt' and set 'SERVER_NAME' to the name of your server.

DO NOT include spaces or any symbols that are not allowed by the policies of a Server name in SQL.

Set DATABASE to the name of your database located in your SQL Server connection, following the
the same rules of typing as well.

To configure Trusted_Connection you need to know your SQL Server settings.

    In case your SQL Server Authentication is enabled (Server Authentication: SQL Server Authentication) this section has to be in 'no' and the user (UID) and password (PWD) has to be defined in the following two sections bellow.

    In case your SQL Server Authentication is using the 'Windows Authentication' this section has to be in 'yes'.

Always respect the syntax of the config file.