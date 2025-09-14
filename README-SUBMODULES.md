# Go-RocketMQ Submodule Guide

This document provides detailed information about how Go-RocketMQ uses Git submodules to manage its modular architecture.

## Overview

Go-RocketMQ follows a modular design where core components are separated into independent modules:

1. **Remoting Module** (`github.com/chenjy16/go-rocketmq-remoting`): Handles network communication and remote procedure calls
2. **Common Module** (`github.com/chenjy16/go-rocketmq-common`): Contains shared data structures and utilities

These modules are integrated as Git submodules in the main repository, allowing for independent development while maintaining a cohesive project structure.

## Submodule Structure

The submodules are located in the `pkg/` directory:

```
go-rocketmq/
├── pkg/
│   ├── common/     # Submodule: github.com/chenjy16/go-rocketmq-common
│   └── remoting/   # Submodule: github.com/chenjy16/go-rocketmq-remoting
```

## Cloning the Repository

When cloning the repository, you need to initialize and update the submodules:

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/your-org/go-rocketmq.git

# Or initialize submodules after cloning
git clone https://github.com/your-org/go-rocketmq.git
cd go-rocketmq
git submodule init
git submodule update
```

## Updating Submodules

To update submodules to their latest versions:

```bash
# Update all submodules to the latest commit
git submodule update --remote

# Update specific submodule
git submodule update --remote pkg/common
git submodule update --remote pkg/remoting
```

## Working with Submodules

### Making Changes to Submodules

If you need to make changes to code in a submodule:

1. Make sure you have write access to the submodule repository
2. Navigate to the submodule directory:
   ```bash
   cd pkg/common
   ```
3. Make your changes and commit them:
   ```bash
   # Make changes to files
   git add .
   git commit -m "Your changes"
   git push origin master
   ```
4. Return to the main repository and update the submodule reference:
   ```bash
   cd ../..
   git add pkg/common
   git commit -m "Update common submodule"
   git push
   ```

### Checking Submodule Status

You can check the status of submodules at any time:

```bash
# Show submodule status
git submodule status

# Show detailed submodule information
git submodule foreach 'echo $name: $(git log -1 --oneline)'
```

## Development Workflow

### For Main Project Developers

1. **Setup Development Environment**:
   ```bash
   make dev-setup
   ```

2. **Build the Project**:
   ```bash
   make build
   ```

3. **Run Tests**:
   ```bash
   make test
   ```

### For Submodule Developers

If you're primarily working on submodule code:

1. **Clone the submodule repository separately**:
   ```bash
   git clone https://github.com/chenjy16/go-rocketmq-common.git
   ```

2. **Make and test your changes** in the separate repository

3. **Push changes** to the submodule repository

4. **Update the main repository** to use the new submodule version:
   ```bash
   cd go-rocketmq
   git submodule update --remote
   git add pkg/common
   git commit -m "Update common submodule to latest version"
   git push
   ```

## Troubleshooting

### Common Issues

1. **Submodule shows as detached HEAD**:
   ```bash
   cd pkg/common
   git checkout master
   ```

2. **Submodule not updating**:
   ```bash
   git submodule sync
   git submodule update --init --recursive
   ```

3. **Forgot to initialize submodules**:
   ```bash
   git submodule init
   git submodule update
   ```

### Best Practices

1. **Always commit submodule updates** in the main repository when you update them
2. **Use specific versions** in production rather than tracking the latest
3. **Coordinate with team members** when updating submodules
4. **Test thoroughly** after submodule updates

## Benefits of Using Submodules

1. **Independent Development**: Each module can be developed and versioned independently
2. **Code Reuse**: Submodules can be used in other projects
3. **Clear Boundaries**: Well-defined interfaces between components
4. **Version Control**: Each module can have its own release cycle
5. **Collaboration**: Teams can work on different modules simultaneously

## Further Reading

- [Git Submodules Documentation](https://git-scm.com/book/en/v2/Git-Tools-Submodules)
- [Go Modules Reference](https://github.com/golang/go/wiki/Modules)