<?php

namespace Bugbyte\Deployer\Shell;

class Shell
{
    /**
     * Asks the user for input
     *
     * @param string $message
     * @param string $default
     * @param boolean $isPassword
     * @return string
     */
    public function inputPrompt($message, $default = '', $isPassword = false)
    {
        fwrite(STDOUT, $message);

        if (!$isPassword)
        {
            $input = trim(fgets(STDIN));
        }
        else
        {
            $input = self::getPassword(false);
            echo PHP_EOL;
        }

        if ($input == '')
            $input = $default;

        return $input;
    }
}
