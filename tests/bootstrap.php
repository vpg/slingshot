<?php
/**
 * PHPUnit bootstrap - load Composer autoloader and optional Phalcon stub for CI
 */
require_once __DIR__ . '/../vendor/autoload.php';

// Stub Phalcon\Di\Injectable when the extension is not installed (e.g. in CI)
if (!class_exists('Phalcon\Di\Injectable', false)) {
    require_once __DIR__ . '/PhalconStub.php';
}
