module.exports.get = () => (process.arch === 'arm64' ? 'ARM64' : 'X86_64')
