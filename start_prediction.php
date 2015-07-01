<?php
for ($user=11; $user <= 90; $user++) {
        exec("python Prediction_Pipeline.py root htvgt794jj 1 ". $user, $output);
}
?>
