# Copyright 2019 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""This launcher module serves as the entry-point of the sample test image.

It decides which test to trigger based upon the arguments provided.
"""

import os
import re
import subprocess
from tempfile import NamedTemporaryFile

from check_notebook_results import NoteBookChecker
from constants import CONFIG_DIR
from constants import DEFAULT_CONFIG
from constants import SCHEMA_CONFIG
from constants import SAMPLE_TEST_DIR
import fire
import kubernetes
import papermill as pm
from run_sample_test import PySampleChecker
import utils
import yamale
import yaml


class SampleTest(object):

    def __init__(self,
                 test_path,
                 results_gcs_dir,
                 function=None,
                 host='',
                 target_image_prefix='',
                 namespace='kubeflow'):
        """Launch a KFP sample_test provided its name.

        :param test_path: path to the corresponding sample test.
        :param results_gcs_dir: gs dir to store test result.
        :param function: the pipeline function to compile
        :param host: host of KFP API endpoint, default is auto-discovery from inverse-proxy-config.
        :param target_image_prefix: prefix of docker image, default is empty.
        :param namespace: namespace for kfp, default is kubeflow.
        """
        self._test_name = None
        self._test_path = test_path
        self._compiled_path = None
        self._results_gcs_dir = results_gcs_dir
        self._function = function
        # Capture the first segment after gs:// as the project name.
        self._target_image_prefix = target_image_prefix
        self._namespace = namespace
        self._host = host
        if self._host == '':
            try:
                # Get inverse proxy hostname from a config map called 'inverse-proxy-config'
                # in the same namespace as KFP.
                try:
                    kubernetes.config.load_incluster_config()
                except:
                    kubernetes.config.load_kube_config()

                self._host = 'http://localhost:8888'
            except Exception as err:
                raise RuntimeError(
                    'Failed to get inverse proxy hostname') from err

        # With the healthz API in place, when the developer clicks the link,
        # it will lead to a functional URL instead of a 404 error.
        print(f'KFP API healthz endpoint is: {self._host}/apis/v1beta1/healthz')

        self._is_notebook = None

        self._sample_test_result = 'junit_Sample%sOutput.xml' % self._test_name
        self._sample_test_output = self._results_gcs_dir

    def _compile(self):
        assert os.path.isfile(self._test_path), "Specified test path is not a valid file: %s" % self._test_path

        # Grab the file name and extension type
        self._test_name, ext_name = os.path.splitext(
            os.path.split(self._test_path)[-1]
        )

        if ext_name == '.py':
            self._is_notebook = False
        elif ext_name == '.ipynb':
            self._is_notebook = True
        else:
            raise RuntimeError("Unrecognized test path file extension: '%s'" % ext_name)

        config_schema = yamale.make_schema(SCHEMA_CONFIG)
        # Retrieve default config
        try:
            with open(DEFAULT_CONFIG, 'r') as f:
                raw_args = yaml.safe_load(f)
            default_config = yamale.make_data(DEFAULT_CONFIG)
            yamale.validate(
                config_schema,
                default_config)  # If fails, a ValueError will be raised.
        except yaml.YAMLError as yamlerr:
            raise RuntimeError('Illegal default config:{}'.format(yamlerr))
        except OSError as ose:
            raise FileExistsError('Default config not found:{}'.format(ose))
        else:
            self._run_pipeline = raw_args['run_pipeline']

        # For presubmit check, do not do any image injection as for now.
        # Notebook samples need to be papermilled first.
        if self._is_notebook:
            # Parse necessary params from config.yaml
            nb_params = {}
            try:
                config_file = os.path.join(CONFIG_DIR,
                                           '%s.config.yaml' % self._test_name)
                with open(config_file, 'r') as f:
                    raw_args = yaml.safe_load(f)
                test_config = yamale.make_data(config_file)
                yamale.validate(
                    config_schema,
                    test_config)  # If fails, a ValueError will be raised.
            except yaml.YAMLError as yamlerr:
                print('No legit yaml config file found, use default args:{}'
                      .format(yamlerr))
            except OSError as ose:
                print(
                    'Config file with the same name not found, use default args:{}'
                    .format(ose))
            else:
                if 'notebook_params' in raw_args.keys():
                    nb_params.update(raw_args['notebook_params'])
                    if 'output' in raw_args['notebook_params'].keys(
                    ):  # output is a special param that has to be specified dynamically.
                        nb_params['output'] = self._sample_test_output
                if 'run_pipeline' in raw_args.keys():
                    self._run_pipeline = raw_args['run_pipeline']

            pm.execute_notebook(
                input_path=self._test_path,
                output_path=self._test_path,
                parameters=nb_params,
                prepare_only=True)
            # Convert to python script.
            return_code = subprocess.call([
                'jupyter', 'nbconvert', '--to', 'python',
                self._test_path
            ])


        # TODO: Does this work for notebooks?
        f = NamedTemporaryFile(delete=False, suffix='.yaml')
        f.close()
        self._compiled_path = f.name
        compile_args = [
            'kfp', 'dsl', 'compile',
            '--py', self._test_path,
            '--output', self._compiled_path
        ]

        if self._function is not None:
            compile_args.append('--function')
            compile_args.append(self._function)
        return_code = subprocess.call(compile_args)

        # Command executed successfully!
        assert return_code == 0

    def _injection(self):
        """Inject images for pipeline components.

        This is only valid for component test
        """
        pass

    def run_test(self):
        self._compile()
        self._injection()

        # Overriding the experiment name of pipeline runs
        experiment_name = self._test_name + '-test'
        os.environ['KF_PIPELINES_OVERRIDE_EXPERIMENT_NAME'] = experiment_name

        if self._is_notebook:
            nbchecker = NoteBookChecker(
                testname=self._test_name,
                result=self._sample_test_result,
                run_pipeline=self._run_pipeline,
                experiment_name=experiment_name,
                host=self._host,
            )
            nbchecker.run()
            os.chdir(SAMPLE_TEST_DIR)
            nbchecker.check()
        else:
            os.chdir(SAMPLE_TEST_DIR)
            pysample_checker = PySampleChecker(
                testname=self._test_name,
                input=self._compiled_path,
                output=self._sample_test_output,
                result=self._sample_test_result,
                host=self._host,
                namespace=self._namespace,
                experiment_name=experiment_name,
            )
            pysample_checker.run()
            pysample_checker.check()


class ComponentTest(SampleTest):
    """Launch a KFP sample test as component test provided its name.

    Currently follows the same logic as sample test for compatibility.
    include xgboost_training_cm
    """

    def __init__(self,
                 test_name,
                 results_gcs_dir,
                 gcp_image,
                 local_confusionmatrix_image,
                 local_roc_image,
                 target_image_prefix='',
                 namespace='kubeflow'):
        super().__init__(
            test_name=test_name,
            results_gcs_dir=results_gcs_dir,
            target_image_prefix=target_image_prefix,
            namespace=namespace)
        self._local_confusionmatrix_image = local_confusionmatrix_image
        self._local_roc_image = local_roc_image
        self._dataproc_gcp_image = gcp_image

    def _injection(self):
        """Sample-specific image injection into yaml file."""
        subs = {  # Tag can look like 1.0.0-rc.3, so we need both "-" and "." in the regex.
            'gcr\.io/ml-pipeline/ml-pipeline/ml-pipeline-local-confusion-matrix:(\w+|[.-])+':
                self._local_confusionmatrix_image,
            'gcr\.io/ml-pipeline/ml-pipeline/ml-pipeline-local-roc:(\w+|[.-])+':
                self._local_roc_image
        }
        if self._test_name == 'xgboost_training_cm':
            subs.update({
                'gcr\.io/ml-pipeline/ml-pipeline-gcp:(\w|[.-])+':
                    self._dataproc_gcp_image
            })

            utils.file_injection('%s.py.yaml' % self._test_name,
                                 '%s.py.yaml.tmp' % self._test_name, subs)
        else:
            # Only the above sample need injection for now.
            pass
        utils.file_injection('%s.py.yaml' % self._test_name,
                             '%s.py.yaml.tmp' % self._test_name, subs)


def main():
    """Launches either KFP sample test or component test as a command
    entrypoint.

    Usage:
    python sample_test_launcher.py sample_test run_test arg1 arg2 to launch sample test, and
    python sample_test_launcher.py component_test run_test arg1 arg2 to launch component
    test.
    """
    fire.Fire({'sample_test': SampleTest, 'component_test': ComponentTest})


if __name__ == '__main__':
    main()
