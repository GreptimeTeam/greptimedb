// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use testcontainers::core::{ContainerState, ExecCommand, WaitFor};

use crate::wal_util::kafka::config::{Config, ZOOKEEPER_PORT};

#[derive(Debug, Clone, Default)]
pub struct ImageArgs;

impl testcontainers::ImageArgs for ImageArgs {
    fn into_iterator(self) -> Box<dyn Iterator<Item = String>> {
        Box::new(
            vec![
                "/bin/bash".to_string(),
                "-c".to_string(),
                format!(
                    r#"
                        echo 'clientPort={}' > zookeeper.properties;
                        echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties;
                        echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties;
                        zookeeper-server-start zookeeper.properties &
                        . /etc/confluent/docker/bash-config &&
                        /etc/confluent/docker/configure &&
                        /etc/confluent/docker/launch
                    "#,
                    ZOOKEEPER_PORT,
                ),
            ]
            .into_iter(),
        )
    }
}

#[derive(Default)]
pub struct Image {
    config: Config,
}

impl testcontainers::Image for Image {
    type Args = ImageArgs;

    fn name(&self) -> String {
        self.config.image_name.clone()
    }

    fn tag(&self) -> String {
        self.config.image_tag.clone()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        self.config.ready_conditions.clone()
    }

    fn env_vars(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        Box::new(self.config.env_vars.iter())
    }

    fn expose_ports(&self) -> Vec<u16> {
        vec![self.config.exposed_port]
    }

    fn exec_after_start(&self, _cs: ContainerState) -> Vec<ExecCommand> {
        vec![]
    }
}
