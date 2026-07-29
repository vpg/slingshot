<?php
/**
 * Minimal Phalcon DI stub for environments without the extension.
 */
namespace Phalcon\Di;

class Injectable
{
    private $di;

    public function setDI($di)
    {
        $this->di = $di;
    }

    public function getDI()
    {
        return $this->di;
    }

    public function __get($service)
    {
        return $this->di->get($service);
    }
}
