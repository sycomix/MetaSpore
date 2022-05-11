//
// Copyright 2022 DMetaSoul
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
//

#include <serving/shared_grpc_context.h>
#include <serving/shared_grpc_server_builder.h>

namespace metaspore::serving {

std::shared_ptr<agrpc::GrpcContext> SharedGrpcContext::get_instance() {
    static std::shared_ptr<agrpc::GrpcContext> instance =
        std::make_shared<agrpc::GrpcContext>(
            SharedGrpcServerBuilder::get_instance()->AddCompletionQueue());
    return instance;
}

}
