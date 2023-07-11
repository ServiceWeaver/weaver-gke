// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	gketool "github.com/ServiceWeaver/weaver-gke/internal/tool"
	"github.com/ServiceWeaver/weaver/runtime/tool"
)

func main() {
	tool.Run("weaver gke", map[string]*tool.Command{
		"deploy":    gketool.DeployCmd(&deploySpec),
		"dashboard": gketool.DashboardCmd(&dashboardSpec),
		"status":    gketool.StatusCmd(&statusSpec),
		"logs":      tool.LogsCmd(&logsSpec),
		"kill":      gketool.KillCmd(&killSpec),
		"store":     gketool.StoreCmd(&storeSpec),
		"profile":   gketool.ProfileCmd(&profileSpec),
		"version":   gketool.VersionCmd("weaver gke"),
		"purge":     &purgeCmd,

		// Hidden commands.
		"controller":  &controllerCmd,
		"distributor": &distributorCmd,
		"manager":     &managerCmd,
		"babysitter":  &babysitterCmd,
	})
}
