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

        if (!$isPassword) {
            $input = trim(fgets(STDIN));
        } else {
            $input = self::getPassword(false);
            echo PHP_EOL;
        }

        if ($input == '') {
            return $default;
        }

        return $input;
    }

    /**
     * Get a password from the shell.
     *
     * This function works on *nix systems only and requires shell_exec and stty.
     *
     * @author http://www.dasprids.de/blog/2008/08/22/getting-a-password-hidden-from-stdin-with-php-cli
     * @param  boolean $stars Wether or not to output stars for given characters
     * @return string
     */
    static protected function getPassword($stars = false)
    {
        // Get current style
        $oldStyle = shell_exec('stty -g');

        if ($stars === false) {
            shell_exec('stty -echo');
            $password = rtrim(fgets(STDIN), "\n");
        } else {
            shell_exec('stty -icanon -echo min 1 time 0');

            $password = '';
            while (true) {
                $char = fgetc(STDIN);

                if ($char === "\n") {
                    break;
                } else if (ord($char) === 127) {
                    if (strlen($password) > 0) {
                        fwrite(STDOUT, "\x08 \x08");
                        $password = substr($password, 0, -1);
                    }
                } else {
                    fwrite(STDOUT, "*");
                    $password .= $char;
                }
            }
        }

        // Reset old style
        shell_exec('stty ' . $oldStyle);

        // Return the password
        return $password;
    }
}
