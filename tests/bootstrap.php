 <?php

// Turn on debug mode - Some tests require it
putenv('YII_DEBUG_MODE=1');

// Load default init code for all entry points.
$config = require __DIR__.'/config.php';

// Autoload the libraries included by composer (e.g. AWS SDK v2)
include_once __DIR__.'/../vendor/autoload.php';

require __DIR__.'/../vendor/yiisoft/yii/framework/yii.php';

Yii::createWebApplication($config);

// Make sure the URL ends with a slash so that we can use relative URLs in test cases
define('TEST_BASE_URL',get_isset_env('TEST_BASE_URL', 'http://172.17.0.1:49190/')); // Our tests run within docker therefore target the docker host

// Needed to run the tests in the yii-aws-sqs extension
define('SQS_ACCESS_KEY', 'AKIAJ33RRQHYL7OL3S5Q');
define('SQS_SECRET_KEY', '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');
