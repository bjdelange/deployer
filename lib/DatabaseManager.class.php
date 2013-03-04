<?php

namespace Bugbyte\Deployer\Database;

use Bugbyte\Deployer\Exceptions\DeployException;
use Bugbyte\Deployer\Exceptions\DatabaseException;
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
     * The relative path to the deployer's sql_updates directory
     *
     * @var string
     */
    protected $sql_updates_path = null;

    /**
     * The hostname of the server that sends commands to the database server
     *
     * @var string
     */
    protected $control_host = null;

    /**
     * The hostname of the database server
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
     * Indicates if the old patches behavior (timestamps) should be used instead of the new behavior (check against db_patches table).
     * This is enabled automatically if the target database has no db_patches table.
     *
     * @var bool
     */
    protected $patches_table_exists = false;

    /**
     * A list of all patches that should just be registered as done in db_patches without being applied.
     *
     * @var array               [timestamp => filepath, ...]
     */
    protected $patches_to_register_as_done = array();

    /**
     * A list of the patches to apply (used for both update and rollback)
     *
     * @var \SQL_update[]       With their full relative paths as keys
     */
    protected $patches_to_apply = array();

    /**
     * Initialization
     *
     * @param \Bugbyte\Deployer\Logger\Logger $logger
     * @param \Bugbyte\Deployer\Shell\Shell $local_shell
     * @param \Bugbyte\Deployer\Shell\RemoteShell $remote_shell
     * @param string $basedir
     * @param string $control_host
     */
    public function __construct(Logger $logger, Shell $local_shell, RemoteShell $remote_shell, $basedir, $control_host)
    {
        $this->logger = $logger;
        $this->local_shell = $local_shell;
        $this->remote_shell = $remote_shell;
        $this->basedir = $basedir;
        $this->control_host = $control_host;

        $this->sql_updates_path = str_replace($this->basedir .'/', '', realpath(__DIR__ .'/../sql_updates'));
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
        // add the directory of the deployer's own patches
        $deployer_dir = $this->sql_updates_path;

        if (!in_array($deployer_dir, $dirs)) {
            $dirs[] = $deployer_dir;
        }

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
     * Checks for the existence of the table db_patches
     *
     * @return bool
     */
    protected function patchTableExists()
    {
        $output = array();
        $return = 0;

        $this->query("SHOW TABLES LIKE 'db_patches'", $output, $return);

        if ($return == 0 && !empty($output)) {
            $this->logger->log('Check if db_patches exists.. yes.', LOG_INFO);
            return true;
        }

        // db_patches table doesn't exist
        $this->logger->log('Check if db_patches exists.. no.', LOG_INFO);
        return false;
    }

    /**
     * Performs database migrations
     *
     * @param string $action             update of rollback
     * @throws \Bugbyte\Deployer\Exceptions\DeployException
     */
    public function check($action)
    {
        $this->logger->log('Database updates:', LOG_INFO, true);

        if (empty($this->database_dirs)) {
            return;
        }

        // collect and verify the database login information so the db_patches table can be checked
        $this->getDatabaseLogin(true);

        $this->patches_table_exists = $this->patchTableExists();

        // make a list of all available patches
        $all_patches = $this->findSQLFilesForPeriod(25300, time(), true);

        if (!$this->patches_table_exists) {
            // old school: select the patches whose timestamps lie between the previous and current deployment timestamps
            if ($action == 'update') {
                $files = $this->findSQLFilesForPeriod($this->last_timestamp, $this->current_timestamp);

                // add the patchfile that creates the db_patcher table
                if (!isset($files[25200])) {
                    $files[25200] = $this->sql_updates_path .'/sql_19700101_080000.class.php';
                }
            }
            elseif ($action == 'rollback') {
                $files = $this->findSQLFilesForPeriod($this->last_timestamp, $this->previous_timestamp);
            }
        } else {
            // get the list of all performed patches from the database
            $patches = $this->findPerformedSQLPatches();

            if (!empty($patches['crashed_update'])) {
                throw new DeployException("Patch(es) ". implode(', ', $patches['crashed_update']) ." have crashed at previous deploy !");
            }

            if (!empty($patches['crashed_rollback'])) {
                throw new DeployException("Patch(es) ". implode(', ', $patches['crashed_rollback']) ." have crashed at previous rollback !");
            }

            $applied_patches = $patches['applied'];

            if ($action == 'update') {
                // find the patches that have not yet been performed
                $files = array_diff_key($all_patches, $applied_patches);

                ksort($files);
            }
            elseif ($action == 'rollback') {
                // find the patches that have been performed on the previous deploy
                $files = $this->findPerformedSQLPatchesFromPeriod($this->last_timestamp, $this->previous_timestamp);
            }
        }

        if (!$this->patches_table_exists) {
            // make a list of all patches that are not a part of this commit (pre-existing) but should be registered as applied
            $patches_to_register_as_done = array();

            foreach ($all_patches as $timestamp => $filename) {
                if (!array_key_exists($timestamp, $files)) {
                    $patches_to_register_as_done[$timestamp] = $filename;
                }
            }
        }

        if ((!isset($files) || empty($files)) && (!isset($patches_to_register_as_done) || empty($patches_to_register_as_done))) {
            return;
        }

        Helper::checkFiles($this->basedir, $files);

        ksort($files, SORT_NUMERIC);

        if ($action == 'update') {
            $action_to_perform = 'Database patches to apply';
            $question_to_ask = 'Apply database patches?';
        } else {
            $action_to_perform = 'Database patches to revert';
            $question_to_ask = 'Rollback database patches?';
        }

        $this->logger->log("$action_to_perform: ". PHP_EOL . implode(PHP_EOL, $files));

        if ($this->local_shell->inputPrompt("$question_to_ask (yes/no): ", 'no') == 'yes') {
            $this->patches_to_apply = $files;
        }

        if (isset($patches_to_register_as_done) && !empty($patches_to_register_as_done)) {
            if ($this->local_shell->inputPrompt('Register the other '. count($patches_to_register_as_done) .' patches as done? (yes/no): ', 'no') == 'yes') {
                $this->patches_to_register_as_done = $patches_to_register_as_done;
            }
        }

        $this->getDatabaseLogin();
    }

    /**
     * Returns all patches that have already been applied, crashed when being applied or crashed when begin reverted
     *
     * @return array  ['applied' => array(..), 'crashed_update' => array(..), 'crashed_rollback' => array(..)]
     */
    protected function findPerformedSQLPatches()
    {
        $list = array('applied' => array(), 'crashed_update' => array(), 'crashed_rollback' => array());

        $output = array();

        $this->query('SELECT patch_name, UNIX_TIMESTAMP(applied_at), UNIX_TIMESTAMP(reverted_at) FROM db_patches ORDER BY patch_timestamp', $output);

        foreach ($output as $patch_record) {
            list($patch_name, $applied_at, $reverted_at) = explode("\t", $patch_record);

            if ($applied_at == 'NULL') {
                $list['crashed_update'][] = $patch_name;
            }
            elseif ($reverted_at != 'NULL') {
                $list['crashed_rollback'][] = $patch_name;
            }
            else {
                $list['applied'][$applied_at] = $patch_name;
            }
        }

        return $list;
    }

    /**
     * Make a list of all database patches applied within a timeframe.
     *
     * @param integer $latest_timestamp
     * @param integer $previous_timestamp
     * @return array
     */
    protected function findPerformedSQLPatchesFromPeriod($latest_timestamp, $previous_timestamp)
    {
        $list = array();
        $output = array();

        $this->query("SELECT patch_name, UNIX_TIMESTAMP(applied_at) FROM db_patches ".
                     "WHERE applied_at BETWEEN FROM_UNIXTIME($previous_timestamp) AND FROM_UNIXTIME($latest_timestamp) ".
                     "ORDER BY patch_timestamp", $output);

        foreach ($output as $patch_record) {
            list($patch_name, $applied_at) = explode("\t", $patch_record);

            $list[$applied_at] = $patch_name;
        }

        return $list;
    }

    /**
     * Voert database migraties uit voor de nieuwste upload
     *
     * @param string $remote_dir
     * @param string $target_dir
     */
    public function update($remote_dir, $target_dir)
    {
        $this->logger->log('updateDatabase', LOG_DEBUG);

        if (!$this->database_checked || empty($this->patches_to_apply)) {
            return;
        }

        $this->sendToDatabase(
            "cd $remote_dir/{$target_dir}; php {$this->database_patcher} update {$this->database_name} {$this->current_timestamp} ". implode(' ', $this->patches_to_apply)
        );

        if (!empty($this->patches_to_register_as_done)) {
            $sql = '';

            foreach ($this->patches_to_register_as_done as $timestamp => $filepath) {
                if ($sql != '') {
                    $sql .= ', ';
                }

                $sql .= "('$filepath', $timestamp, FROM_UNIXTIME($timestamp))";
            }

            $this->query("INSERT INTO db_patches (patch_name, patch_timestamp, applied_at) VALUES $sql;");
        }
    }

    /**
     * Reverts database migrations to the previous deployment
     *
     * @param string $remote_dir
     * @param string $previous_target_dir
     */
    public function rollback($remote_dir, $previous_target_dir)
    {
        $this->logger->log('rollbackDatabase', LOG_DEBUG);

        if (!$this->database_checked || empty($this->patches_to_apply)) {
            return;
        }

        $this->sendToDatabase(
            "cd $remote_dir/{$previous_target_dir}; php {$this->database_patcher} rollback {$this->database_name} {$this->current_timestamp} ". implode(' ', $this->patches_to_apply)
        );
    }

    /**
     * Prompt the user to enter the database name, login and password to use on the remote server for executing the database patches.
     *
     * @param bool $pre_check       If this is just a check to access te database (to check the db_patches table) or asking for confirmation to send the changes
     */
    protected function getDatabaseLogin($pre_check = false)
    {
        if ($this->database_checked) {
            return;
        }

        $database_name = $this->database_name;

        // if the database credentials are known, no need to ask for them again
        if ($database_name === null) {
            $update_database = $this->local_shell->inputPrompt('Check if database needs updates? (yes/no): ', 'no');

            if ($update_database != 'yes') {
                $database_name = 'skip';
            }
        }

        if ($database_name != 'skip') {
            if ($this->database_name !== null) {
                // we're not updating anything yet, so no need to ask questions
                if (!$pre_check) {
                    $update_database = $this->local_shell->inputPrompt('Update database '. $this->database_name .'? (yes/no): ', 'no');

                    if ($update_database != 'yes') {
                        $database_name = 'skip';
                    }
                }
            } else {
                $database_name = $this->local_shell->inputPrompt('Database name [skip]: ', 'skip');
            }
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

            $return = 0;
            $output = array();

            // controleren of deze gebruiker een tabel mag aanmaken (rudimentaire toegangstest)
            $this->query("CREATE TABLE `temp_{$this->current_timestamp}` (`field1` INT NULL); DROP TABLE `temp_{$this->current_timestamp}`;", $output, $return, $database_name, $username, $password);

            if ($return != 0) {
                return $this->getDatabaseLogin();
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
     * @param string $command
     * @param array $output
     * @param integer $return
     * @param string $database_name
     * @param string $username
     * @param string $password
     * @throws \Bugbyte\Deployer\Exceptions\DatabaseException
     */
    public function sendToDatabase($command, &$output = array(), &$return = 0, $database_name = null, $username = null, $password = null)
    {
        if ($this->database_checked && $this->database_name == 'skip') {
            return;
        }

        if ($database_name === null) {
            $database_name = $this->database_name;
        }

        if ($username === null) {
            $username = $this->database_user;
        }

        if ($password === null) {
            $password = $this->database_pass;
        }

        $this->remote_shell->sshExec(
            $this->control_host,
            "$command | mysql -h{$this->database_host} -u$username -p$password $database_name", $output, $return, '/ -p[^ ]+ /', ' -p***** '
        );

        if ($return !== 0) {
            throw new DatabaseException('Database interaction failed !');
        }
    }

    /**
     * Wrapper for sendToDatabase() to send plain commands to the database
     *
     * @param string$command
     * @param array $output
     * @param int $return
     * @param string $database_name
     * @param string $username
     * @param string $password
     * @throws \Bugbyte\Deployer\Exceptions\DatabaseException
     */
    public function query($command, &$output = array(), &$return = 0, $database_name = null, $username = null, $password = null)
    {
        if ($this->database_checked && $this->database_name == 'skip') {
            return;
        }

        if ($database_name === null) {
            $database_name = $this->database_name;
        }

        if ($username === null) {
            $username = $this->database_user;
        }

        if ($password === null) {
            $password = $this->database_pass;
        }

        $command = str_replace(array(/*'(', ')',*/ '`'), array(/*'\(', '\)',*/ '\`'), $command);
        $command = escapeshellarg($command);

        $this->remote_shell->sshExec(
            $this->control_host,
            "mysql -h{$this->database_host} -u$username -p$password -e $command $database_name",
            $output, $return, '/ -p[^ ]+ /', ' -p***** '
        );

        if ($return !== 0) {
            throw new DatabaseException('Database interaction failed !');
        }
    }

    /**
     * Makes a list of all SQL update files within the timeframe, in the order the start- and endtime imply:
     *   if the starttime is *before* the endtime it's an update cycle and the updates are ordered chronologically (old to new).
     *   if the starttime is *after* the endtime it's a rollback and the updates are reversed (new to old).
     *
     * @param integer $starttime (timestamp)
     * @param integer $endtime (timestamp)
     * @param boolean $quiet
     * @throws DeployException
     * @return array
     */
    public function findSQLFilesForPeriod($starttime, $endtime, $quiet = false)
    {
        $previous_quiet = $this->logger->setQuiet($quiet);

        $this->logger->log('findSQLFilesForPeriod('. date('Y-m-d H:i:s', $starttime) .','. date('Y-m-d H:i:s', $endtime) .')', LOG_DEBUG);

        if (empty($this->database_dirs)) {
            $this->logger->setQuiet($previous_quiet);
            return array();
        }

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
                    $timestamp = Helper::convertFilenameToTimestamp($entry->getFilename());

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

                if (!$quiet) {
                    $this->logger->log($count_files .' SQL update patch'. ($count_files > 1 ? 'es' : '') .' between '. date('Y-m-d H:i:s', $starttime) .' and '. date('Y-m-d H:i:s', $endtime) .':');
                }
            } else {
                krsort($update_files, SORT_NUMERIC);

                if (!$quiet) {
                    $this->logger->log($count_files .' SQL rollback patch'. ($count_files > 1 ? 'es' : '') .' between '. date('Y-m-d H:i:s', $starttime) .' and '. date('Y-m-d H:i:s', $endtime) .':');
                }
            }

            $this->logger->log($update_files);
        } else {
            if (!$quiet) {
                $this->logger->log('No SQL patches between '. date('Y-m-d H:i:s', $starttime) .' and '. date('Y-m-d H:i:s', $endtime));
            }
        }

        $this->logger->setQuiet($previous_quiet);

        return $update_files;
    }
}
