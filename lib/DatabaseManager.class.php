<?php

namespace Bugbyte\Deployer\Database;

use Bugbyte\Deployer\Exceptions\DeployException;
use Bugbyte\Deployer\Shell\Shell;
use Bugbyte\Deployer\Shell\RemoteShell;
use Bugbyte\Deployer\Logger\Logger;

class DatabaseManager
{
    /**
     * @var Logger
     */
    protected $logger = null;

    /**
     * @var Shell
     */
    protected $local_shell = null;

    /**
     * @var RemoteShell
     */
    protected $remote_shell = null;

    /**
     * Het pad van de database patcher, relatief vanaf de project root
     *
     * @var string
     */
    protected $database_patcher = null;

    /**
     * Alle directories die moeten worden doorzocht naar SQL update files
     *
     * @var array
     */
    protected $database_dirs = array();

    /**
     * The root directory of the project
     *
     * @var string
     */
    protected $basedir = null;

    /**
     * De hostname van de database server
     *
     * @var string
     */
    protected $database_host = null;

    /**
     * De naam van de database waar de SQL updates naartoe gaan
     *
     * @var string
     */
    protected $database_name = null;

    /**
     * De gebruikersnaam van de database
     *
     * @var string
     */
    protected $database_user = null;

    /**
     * Het wachtwoord dat bij de gebruikersnaam hoort
     *
     * @var string
     */
    protected $database_pass = null;

    /**
     * Of de database-gegevens gecontroleerd zijn
     *
     * @var boolean
     */
    protected $database_checked = false;

    /**
     * @var integer
     */
    protected $current_timestamp = null;

    /**
     * @var integer
     */
    protected $previous_timestamp = null;

    /**
     * @var integer
     */
    protected $last_timestamp = null;

    /**
     * Initialization
     *
     * @param \Bugbyte\Deployer\Logger\Logger $logger
     * @param \Bugbyte\Deployer\Shell\Shell $local_shell
     * @param \Bugbyte\Deployer\Shell\RemoteShell $remote_shell
     * @param string $basedir
     */
    public function __construct(Logger $logger, Shell $local_shell, RemoteShell $remote_shell, $basedir)
    {
        $this->logger = $logger;
        $this->local_shell = $local_shell;
        $this->remote_shell = $remote_shell;
        $this->basedir = $basedir;
    }

    /**
     * Initialization
     *
     * @param string $patcher
     */
    public function setPatcher($patcher)
    {
        $this->database_patcher = $patcher;
    }

    /**
     * Initialization
     *
     * @param array $dirs
     */
    public function setDirs(array $dirs)
    {
        $this->database_dirs = $dirs;
    }

    /**
     * Initialization
     *
     * @param string $host
     */
    public function setHost($host)
    {
        $this->database_host = $host;
    }

    /**
     * Initialization
     *
     * @param string $database_name
     */
    public function setDatabaseName($database_name)
    {
        $this->database_name = $database_name;
    }

    /**
     * Initialization
     *
     * @param string $username
     */
    public function setUsername($username)
    {
        $this->database_user = $username;
    }

    /**
     * Initialization
     *
     * @param string $password
     */
    public function setPassword($password)
    {
        $this->database_pass = $password;
    }

    /**
     * Check the database credentials and if the db_patches table exists
     *
     * @param int $current_timestamp
     * @param int $previous_timestamp
     * @param int $last_timestamp
     */
    public function initialize($current_timestamp, $previous_timestamp, $last_timestamp)
    {
        $this->current_timestamp = $current_timestamp;
        $this->previous_timestamp = $previous_timestamp;
        $this->last_timestamp = $last_timestamp;
    }

    /**
     * Voert database migraties uit voor de nieuwste upload
     *
     * @param string $remote_host
     * @param string $action             update of rollback
     */
    public function checkDatabase($remote_host, $action)
    {
        $this->logger->log('Database updates:', LOG_INFO, true);

        if (empty($this->database_dirs)) {
            return;
        }

        if ($action == 'update') {
            $files = $this->findSQLFilesForPeriod($this->last_timestamp, $this->current_timestamp);
        }
        elseif ($action == 'rollback') {
            $files = $this->findSQLFilesForPeriod($this->last_timestamp, $this->previous_timestamp);
        }

        if (!isset($files) || !$files) {
            return;
        }

        static::checkDatabaseFiles($this->basedir, $files);

        $this->getDatabaseLogin($remote_host);
    }

    /**
     * Voert database migraties uit voor de nieuwste upload
     *
     * @param string $remote_host
     * @param string $remote_dir
     * @param string $target_dir
     */
    public function updateDatabase($remote_host, $remote_dir, $target_dir)
    {
        $this->logger->log('updateDatabase', LOG_DEBUG);

        if (!($files = $this->findSQLFilesForPeriod($this->last_timestamp, $this->current_timestamp))) {
            return;
        }

        self::checkDatabaseFiles($this->basedir, $files);

        $this->getDatabaseLogin($remote_host, $this->database_host);

        $output = array();
        $return = null;
        $this->sendToDatabase($remote_host, $this->database_host, "cd $remote_dir/{$target_dir}; php {$this->database_patcher} update {$this->database_name} ". implode(' ', $files), $output, $return, $this->database_name, $this->database_user, $this->database_pass);
    }

    /**
     * Reverts database migrations to the previous deployment
     *
     * @param string $remote_host
     * @param string $database_host
     * @param string $remote_dir
     */
    public function rollbackDatabase($remote_host, $database_host, $remote_dir)
    {
        $this->logger->log('rollbackDatabase', LOG_DEBUG);

        if (!($files = $this->findSQLFilesForPeriod($this->last_timestamp, $this->previous_timestamp))) {
            return;
        }

        self::checkDatabaseFiles($this->basedir, $files);

        $this->getDatabaseLogin($remote_host, $database_host);

        $this->sendToDatabase($remote_host, $database_host, "cd $remote_dir/{$this->last_remote_target_dir}; php {$this->database_patcher} rollback ". implode(' ', $files), $output, $return, $this->database_name, $this->database_user, $this->database_pass);
    }

    /**
     * Controleert of alle opgegeven bestanden bestaan en de juist class en sql code bevatten
     *
     * @param string $path_prefix
     * @param array $filenames
     * @throws DeployException
     * @return array				De absolute paden van alle files
     */
    static public function checkDatabaseFiles($path_prefix, $filenames)
    {
        $classes = array();

        foreach ($filenames as $filename)
        {
            $filepath = $path_prefix .'/'. $filename;

            if (!file_exists($filepath)) {
                throw new DeployException("$filepath not found");
            }

            $classname = str_replace('.class', '', pathinfo($filename, PATHINFO_FILENAME));

            require_once $filepath;

            if (!class_exists($classname)) {
                throw new DeployException("Class $classname not found in $filepath");
            }

            $sql = new $classname();

            if (!$sql instanceof \SQL_update) {
                throw new DeployException("Class $classname doesn't implement SQL_update");
            }

            $up_sql = trim($sql->up());

            if ($up_sql != '' && substr($up_sql, -1) != ';') {
                throw new DeployException("$classname up() code doesn't end with ';'");
            }

            $down_sql = trim($sql->down());

            if ($down_sql != '' && substr($down_sql, -1) != ';') {
                throw new DeployException("$classname down() code doesn't end with ';'");
            }

            $classes[] = $sql;
        }

        return $classes;
    }

    /**
     * Prompt the user to enter the database name, login and password to use on the remote server for executing the database patches.
     *
     * @param string $remote_host
     */
    protected function getDatabaseLogin($remote_host)
    {
        if ($this->database_checked) {
            return;
        }

        if ($this->database_name !== null) {
            $database_name = $this->local_shell->inputPrompt('Update database '. $this->database_name .' (yes/no): ', 'no');

            if ($database_name == 'yes') {
                $database_name = $this->database_name;
            } else {
                $database_name = 'skip';
            }
        } else {
            $database_name = $this->local_shell->inputPrompt('Database [skip]: ', 'skip');
        }

        if ($database_name == '' || $database_name == 'no') {
            $database_name = 'skip';
        }

        if ($database_name == 'skip') {
            $username = '';
            $password = '';

            $this->logger->log('Skip database patches');
        }
        else {
            $username = $this->database_user !== null ? $this->database_user : $this->local_shell->inputPrompt('Database username [root]: ', 'root');
            $password = $this->database_pass !== null ? $this->database_pass : $this->local_shell->inputPrompt('Database password: ', '', true);

            // controleren of deze gebruiker een tabel mag aanmaken (rudimentaire toegangstest)
            $this->sendToDatabase($remote_host, $this->database_host, "echo '". addslashes("CREATE TABLE temp_{$this->current_timestamp} (field1 INT NULL); DROP TABLE temp_{$this->timestamp};") ."'", $output, $return, $database_name, $username, $password);

            if ($return != 0) {
                return $this->getDatabaseLogin($remote_host, $this->database_host);
            }

            $this->logger->log('Database check passed');
        }

        $this->database_checked = true;
        $this->database_name = $database_name;
        $this->database_user = $username;
        $this->database_pass = $password;
    }

    /**
     * Send a query to the database.
     *
     * @param string $remote_host
     * @param string $database_host
     * @param string $command
     * @param array $output
     * @param integer $return
     * @param string $database_name
     * @param string $username
     * @param string $password
     */
    protected function sendToDatabase($remote_host, $database_host, $command, &$output, &$return, $database_name, $username, $password)
    {
        if ($this->database_checked && $this->database_name == 'skip') {
            return;
        }

        $this->remote_shell->sshExec($remote_host, "$command | mysql -h$database_host -u$username -p$password $database_name", $output, $return, '/ -p[^ ]+ /', ' -p***** ');
    }

    /**
     * Makes a list of all SQL update files within the timeframe, in the order the start- and endtime imply:
     *   if the starttime is *before* the endtime it's an update cycle and the updates are ordered chronologically (old to new).
     *   if the starttime is *after* the endtime it's a rollback and the updates are reversed (new to old).
     *
     * @param integer $starttime (timestamp)
     * @param integer $endtime (timestamp)
     * @throws DeployException
     * @return array
     */
    public function findSQLFilesForPeriod($starttime, $endtime)
    {
        $this->logger->log('findSQLFilesForPeriod('. date('Y-m-d H:i:s', $starttime) .','. date('Y-m-d H:i:s', $endtime) .')', LOG_DEBUG);

        $reverse = $starttime > $endtime;

        if ($reverse) {
            $starttime2 = $starttime;
            $starttime = $endtime;
            $endtime = $starttime2;
            unset($starttime2);
        }

        $update_files = array();

        foreach ($this->database_dirs as $database_dir) {
            $dir = new \DirectoryIterator($database_dir);

            foreach ($dir as $entry) {
                /** @var \SplFileInfo|\DirectoryIterator $entry */

                if ($entry->isDot() || !$entry->isFile()) {
                    continue;
                }

                if (preg_match('/sql_(\d{8}_\d{6})\.class.php/', $entry->getFilename(), $matches)) {
                    if (!($timestamp = strtotime(preg_replace('/(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})/', '$1-$2-$3 $4:$5:$6', $matches[1])))) {
                        throw new DeployException("Can't convert {$matches[1]} to timestamp");
                    }

                    if ($timestamp > $starttime && $timestamp < $endtime) {
                        $update_files[$timestamp] = $entry->getPathname();
                    }
                }
            }
        }

        if (!empty($update_files)) {
            $count_files = count($update_files);

            if (!$reverse) {
                ksort($update_files, SORT_NUMERIC);

                $this->logger->log($count_files .' SQL update patch'. ($count_files > 1 ? 'es' : '') .' between '. date('Y-m-d H:i:s', $starttime) .' and '. date('Y-m-d H:i:s', $endtime) .':');
            } else {
                krsort($update_files, SORT_NUMERIC);

                $this->logger->log($count_files .' SQL rollback patch'. ($count_files > 1 ? 'es' : '') .' between '. date('Y-m-d H:i:s', $starttime) .' and '. date('Y-m-d H:i:s', $endtime) .':');
            }

            $this->logger->log($update_files);
        } else {
            $this->logger->log('No SQL patches between '. date('Y-m-d H:i:s', $starttime) .' and '. date('Y-m-d H:i:s', $endtime));
        }

        return $update_files;
    }
}
