 SET NOCOUNT ON
 
 declare 
    @restoreFromDir varchar(255),                   --The directory where the database backups are located. Do not include a trailing slash.
    @restoreToDataDir nvarchar(512)= null,          --The directory where the data files (i.e. MDF) will be restored to. Leave blank to use the default database folder.
    @restoreToLogDir nvarchar(512) = null,          --The directory where the log files (i.e. LDF) will be restored to. Leave blank to use the default log folder.
    @restoreToSecondaryDataDir nvarchar(512) = null,    --Allows NDF files to be placed in a different location. Leave blank to use @restoreToDataDir instead.
    @recovery int = 1,                              --Set to 1 to use the option WITH RECOVERY, or 0 for WITH NORECOVERY
    @MatchFileList char(1) = 'N',                   --Set to 'Y' to restore to the same directory structure that is contained in the backup, creating the folders if necessary, and ignoring the @restoreToDataDir / @restoreToLogDir values.                                                --Also allows for secondary data files 'ndf' to to be in a different dir than mdf files
    @OneDBName varchar(255) = null,                 --Filters the list of .BAKs to just this single name. Takes the latest .BAK/.DAT file with this name.
    @bitReplace_Existing_DB bit = 0                 --Set to 1 to overwrite existing databases.
 
 
set  @restoreFromDir = 'B:\backup\prodbackupsforrefresh\PTPRDDB1\' set   @MatchFileList = 'Y' set  @bitReplace_Existing_DB = 1

DECLARE 
    @filename varchar(255),
    @cmd varchar(8000),
    @cmd2 varchar(500),
    @cmd3 varchar(255),
    @DataName varchar (255),
    @LogName varchar (255),
    @LogicalName varchar(255),
    @PhysicalName varchar(255),
    @Type varchar(20),
    @FileGroupName varchar(255),
    @Size varchar(20),
    @MaxSize varchar(20),
    @restoreToDir varchar(255),
    @searchName varchar(255),
    @dbName varchar(255),
    @PhysicalFileName varchar(255)


DECLARE @dirList TABLE (filename varchar(100))


DECLARE @filelist TABLE 
(
LogicalName varchar(255), 
PhysicalName varchar(255), 
Type varchar(20), 
FileGroupName varchar(255), 
Size varchar(20), 
MaxSize varchar(20),
FileId int,
CreateLSN bit, 
DropLSN bit, 
UniqueID varchar(255),
ReadOnlyLSn bit, 
ReadWriteLSN bit, 
backupSizeInBytes varchar(50), 
SourceBlockSize int,
FileGroupid Int, 
LogGroupGUID varchar(255),
DifferentialBaseLSN varchar(255),
DifferentialBaseGUID varchar(255),
isReadOnly bit, 
IsPresent bit,
TDEThumbprint varchar(255),
SnapshotUrl varchar(255) 
)

DECLARE @Dbnameheaders TABLE 
(
BackupName varchar(256), 
BackupDescription varchar(256), 
BackupType varchar(256), 
ExpirationDate varchar(256), 
Compressed varchar(256), 
Position varchar(256),
DeviceType varchar(256), 
UserName varchar(256), 
ServerName varchar(256), 
DatabaseName varchar(256), 
DatabaseVersion varchar(256), 
DatabaseCreationDate varchar(256),
BackupSize varchar(256), 
FirstLSN varchar(256), 
LastLSN varchar(256), 
CheckpointLSN varchar(256), 
DatabaseBackupLSN varchar(256), 
BackupStartDate varchar(256),
BackupFinishDate varchar(256), 
SortOrder varchar(256), 
CodePage varchar(256), 
UnicodeLocaleId varchar(256), 
UnicodeComparisonStyle varchar(256), 
CompatibilityLevel varchar(256),
SoftwareVendorId varchar(256), 
SoftwareVersionMajor varchar(256), 
SoftwareVersionMinor varchar(256), 
SoftwareVersionBuild varchar(256), 
MachineName varchar(256), 
Flags varchar(256),
BindingID varchar(256), 
RecoveryForkID varchar(256), 
Collation varchar(256), 
FamilyGUID varchar(256), 
HasBulkLoggedData varchar(256), 
IsSnapshot varchar(256), 
IsReadOnly varchar(256),
IsSingleUser varchar(256), 
HasBackupChecksums varchar(256), 
IsDamaged varchar(256), 
BeginsLogChain varchar(256), 
HasIncompleteMetaData varchar(256), 
IsForceOffline varchar(256),
IsCopyOnly varchar(256), 
FirstRecoveryForkID varchar(256), 
ForkPointLSN varchar(256), 
RecoveryModel varchar(256), 
DifferentialBaseLSN varchar(256), 
DifferentialBaseGUID varchar(256),
BackupTypeDescription varchar(256), 
BackupSetGUID varchar(256), 
CompressedBackupSize varchar(256), 
Containment varchar(256),
KeyAlgorithm nvarchar(32),
EncryptorThumbprint varbinary(20),
EncryptorType nvarchar(32)
);
 
--Process parameters
IF RIGHT(@restoreFromDir,1) = '\'
    SET @restoreFromDir = LEFT(@restoreFromDir, LEN(@restoreFromDir)-1)

IF ISNULL(@restoreToDataDir,'') = ''
    SET @restoreToDataDir = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS nvarchar(512))
    --EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultData', @restoreToDataDir output

IF ISNULL(@restoreToLogDir,'') = ''
    SET @restoreToLogDir = CAST(SERVERPROPERTY('InstanceDefaultLogPath') AS nvarchar(512))
    --EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultLog', @restoreToLogDir output

IF ISNULL(@restoreToSecondaryDataDir, '') = ''
    SET @restoreToSecondaryDataDir = @restoreToDataDir


--SELECT @restoreToDataDir, @restoreToLogDir, @restoreToSecondaryDataDir



--Get the list of database backups that are in the restoreFromDir directory. We only go with FULL backups and ignore DIFF's or TRN's.
IF @OneDBName IS NULL
    SELECT @cmd = 'dir /b /on ' + @restoreFromDir + '\*.*'
ELSE
    SELECT @cmd = 'dir /b /o-d /o-g ' + @restoreFromDir + '\*.*'


--select @cmd,'AllFiles' -- Give All Files in Backup Folder

INSERT @dirList EXEC master..xp_cmdshell @cmd

 

IF @OneDBName IS NULL
    BEGIN
        DECLARE BakFile_csr cursor for
        SELECT * FROM @dirList where filename like '%.bak' or filename like '%.dat' order by filename
    END
ELSE
    BEGIN -- single db, don't order by filename, take default latest date /o-d parm in dir command above
        SELECT @searchName = @OneDBName + '_FULL_%'

        DECLARE BakFile_csr cursor for
        SELECT top 1 * FROM @dirList WHERE filename LIKE @searchName
    END


OPEN BakFile_csr
FETCH BakFile_csr INTO @filename

WHILE @@fetch_status = 0
    BEGIN
 
        SELECT @cmd = 'RESTORE FILELISTONLY FROM disk = ''' + @restoreFromDir + '\' + @filename + ''''
 
        INSERT @filelist EXEC ( @cmd )
	 
        --identify the db name from backup file
        SELECT @cmd3 = 'RESTORE HEADERONLY FROM disk = ''' + @restoreFromDir + '\' + @filename + ''''
        INSERT @Dbnameheaders EXEC (@cmd3)
 
        SELECT @dbName = DatabaseName FROM @Dbnameheaders
        --identify the db name from backup file [END Here]
 
        IF @OneDBName is null
            SELECT @dbName = @dbName --left(@filename,datalength(@filename) - patindex('%_FULL_%',reverse(@filename))-3)
        ELSE
            SELECT @dbName = @OneDBName

        SELECT @cmd = 'RESTORE DATABASE [' + @dbName + '] FROM DISK = ''' + @restoreFromDir + '\' + @filename + ''' WITH '
        --Example: RESTORE DATABASE [HCPPREPROD] FROM DISK = 'C:\To Restore\HCPPREPROD_FULL_20180103.dat' WITH 
 
        PRINT '---RESTORING DATABASE ' + @dbName

        --Select * from @filelist ---List of files in backupfile all mdf,ndf,ldf


        DECLARE DataFileCursor cursor for
        SELECT LogicalName, PhysicalName, Type, FileGroupName, Size, MaxSize
        FROM @filelist

	 
        OPEN DataFileCursor
        FETCH DataFileCursor INTO @LogicalName, @PhysicalName, @Type, @FileGroupName, @Size, @MaxSize

        WHILE @@fetch_status = 0
            BEGIN 
		 
                IF @MatchFileList != 'Y'
                    BEGIN -- RESTORE with MOVE option
					
                        SELECT @PhysicalFileName = REVERSE(SUBSTRING(REVERSE(RTRIM(@PhysicalName)),1,patindex('%\%',reverse(rtrim(@PhysicalName)))-1 ))

                        IF @Type = 'L'
                            SELECT @restoreToDir = @restoreToLogDir
                        ELSE IF @PhysicalFileName LIKE '%.ndf'
                            SELECT @restoreToDir = @restoreToSecondaryDataDir
                        ELSE
                            SELECT @restoreToDir = @restoreToDataDir

                        IF RIGHT(@restoreToDir, 1) = '\'
                            SET @restoreToDir = LEFT(@restoreToDir, LEN(@restoreToDir)-1)

                        --SELECT @LogicalName, @restoreToDir, @PhysicalFileName
                        SELECT @cmd = @cmd + ' MOVE ' + @LogicalName + ' TO ''' + @restoreToDir + '\' + @PhysicalFileName + ''', '

                    END
                ELSE
                    BEGIN -- Match the file list, attempt to create any missing directory
				
                        SELECT @restoreToDir = left(@PhysicalName,datalength(@PhysicalName) - patindex('%\%',reverse(@PhysicalName)) )
                        SELECT @cmd2 = 'if not exist ' +@restoreToDir+ ' md ' +@restoreToDir
                        EXEC master..xp_cmdshell @cmd2
                    END


                FETCH DataFileCursor INTO @LogicalName, @PhysicalName, @Type, @FileGroupName, @Size, @MaxSize

            END -- DataFileCursor loop

        CLOSE DataFileCursor
        DEALLOCATE DataFileCursor



        IF @recovery = 0
            SELECT @cmd = @cmd + ' NORECOVERY, STATS = 5'
        ELSE
            SELECT @cmd = @cmd + ' RECOVERY, STATS = 5'



        IF @bitReplace_Existing_DB = 1
            SELECT @cmd = @cmd + ', REPLACE'
			  select @cmd
 

        IF @bitReplace_Existing_DB = 0 AND DB_ID(@dbName) IS NOT NULL
            PRINT 'Database already exists - skipping... ' + @dbName
        ELSE
          --  EXEC (@cmd)
		  print  @cmd

		-- select  @cmd

        DELETE FROM @filelist
        DELETE FROM @Dbnameheaders

        FETCH BakFile_csr INTO @filename

    END -- BakFile_csr loop

CLOSE BakFile_csr
DEALLOCATE BakFile_csr



 