<?php
/**
 * Minimal stubs for Phalcon classes when the extension is not installed (e.g. in CI).
 * Used only so that Slingshot can be loaded and tested without the real Phalcon extension.
 */
namespace Phalcon\Di {
    class Injectable
    {
        public $logger;
    }
}

namespace Phalcon\Logger {
    class Logger
    {
        public function info($msg) {}
        public function debug($msg) {}
    }
}
