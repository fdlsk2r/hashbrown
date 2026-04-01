# 刷新上游的tags
flush_upstream_tags:
	git fetch upstream --tags --force
	git --no-pager tag --sort=v:refname

# 基于特定版本fork特定分支
fork_version:
	git checkout tags/v0.16.1 -b v0.16.1-goscript

# 将旧分支的改动 Cherry-pick 过
merge_version:
	git cherry-pick