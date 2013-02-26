<?php

namespace Bugbyte\Deployer\Logger;

class Logger
{
    /**
     * If the deployer is run in debugging mode (more verbose output)
     *
     * @var bool
     */
    protected $debug = false;

    /**
     * Het pad van de logfile, als logging gewenst is
     *
     * @var string
     */
    protected $logfile = null;

    /**
     * Initialization
     *
     * @param string $logfile
     * @param bool $debug
     */
    public function __construct($logfile = null, $debug = false)
    {
        $this->logfile = $logfile;
        $this->debug = $debug;
    }

    /**
     * Output wrapper
     *
     * @param string $message
     * @param integer $level		  LOG_INFO (6)  = normal (always show),
     *								LOG_DEBUG (7) = debugging (hidden by default)
     * @param bool $extra_newline	 Automatisch een newline aan het eind toevoegen
     */
    public function log($message, $level = LOG_INFO, $extra_newline = false)
    {
        if (is_array($message)) {
            if (count($message) == 0) {
                return;
            }

            $message = implode(PHP_EOL, $message);
        }

        if ($level == LOG_INFO || ($this->debug && $level == LOG_DEBUG)) {
            echo $message . PHP_EOL;

            if ($extra_newline) {
                echo PHP_EOL;
            }
        }

        if ($this->logfile) {
            error_log($message . PHP_EOL, 3, $this->logfile);
        }
    }
}
