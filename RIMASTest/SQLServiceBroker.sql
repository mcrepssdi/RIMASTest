
--Enable Service Brokers
ALTER DATABASE MessageQue SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
SELECT name, database_id, is_broker_enabled FROM sys.databases WHERE name = 'MessageQue'

-- Build Service Broker
CREATE QUEUE AddressBookQueue
CREATE MESSAGE TYPE AddressBook VALIDATION = NONE
CREATE CONTRACT AddressBookContract AUTHORIZATION [dbo] (AddressBook SENT BY ANY)
CREATE SERVICE AddressBookService ON QUEUE AddressBookQueue (AddressBookContract)

-- SEND NEW MESSAGE VIA TRIGGER
DECLARE @AccountNo VARCHAR(15)
DECLARE @Action VARCHAR(15)
DECLARE @Message VARCHAR(MAX)
DECLARE @id AS uniqueidentifier
SET @id = NEWID(); 

Set @Action = 'INSERT';
SET @AccountNo = '100900';
SET @Message = (SELECT @AccountNo As AccountNo, @Action As Action, @id AS Id FOR JSON PATH);

DECLARE @handle AS uniqueidentifier
BEGIN DIALOG CONVERSATION @handle
FROM SERVICE [AddressBookService]
TO SERVICE 'AddressBookService'
ON CONTRACT AddressBookContract
WITH ENCRYPTION = OFF;
    
SEND ON CONVERSATION @handle
MESSAGE TYPE AddressBook (@Message)

END CONVERSATION @handle WITH CLEANUP
INSERT INTO [MessageQue].[dbo].[AMasterSample] ([AccountNo],[Action],LastUpdated,Handle) VALUES (@AccountNo, @Action, GETDATE(), @id);


-- SQL TO VIEW QUEUE
SELECT TOP (1000) *, casted_message_body =
                     CASE message_type_name WHEN 'X'
                                                THEN CAST(message_body AS NVARCHAR(MAX))
                                            ELSE message_body
                         END
FROM [MessageQue].[dbo].[AddressBookQueue] WITH(NOLOCK)

-- READS and POPS FROM QUEUE
    RECEIVE TOP(1) CAST(message_body AS VARCHAR(MAX)) AS message_body FROM dbo.[AddressBookQueue]