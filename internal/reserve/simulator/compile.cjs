// Run: node compile.cjs /tmp/reserve-solc/node_modules/solc
const fs=require('fs'); const path=require('path'); const crypto=require('crypto');
const solc=require(process.argv[2]);
if(solc.version()!=='0.8.28+commit.7893614a.Emscripten.clang')throw Error('unexpected compiler');
const source=fs.readFileSync(path.join(__dirname,'RouteProbe.sol'),'utf8');
const settings={optimizer:{enabled:true,runs:200},viaIR:true,evmVersion:'cancun',metadata:{bytecodeHash:'none'},outputSelection:{'*':{'*':['abi','evm.deployedBytecode.object']}}};
const output=JSON.parse(solc.compile(JSON.stringify({language:'Solidity',sources:{'RouteProbe.sol':{content:source}},settings})));
for(const e of output.errors||[]) {process.stderr.write(e.formattedMessage); if(e.severity==='error')process.exit(1);}
const artifact=output.contracts['RouteProbe.sol'].RouteProbe;
fs.writeFileSync(path.join(__dirname,'probe-abi.json'),JSON.stringify(artifact.abi));
fs.writeFileSync(path.join(__dirname,'probe-runtime.hex'),artifact.evm.deployedBytecode.object+'\n');
fs.writeFileSync(path.join(__dirname,'compiler.json'),JSON.stringify({compiler:solc.version(),source_sha256:crypto.createHash('sha256').update(source).digest('hex'),runtime_sha256:crypto.createHash('sha256').update(Buffer.from(artifact.evm.deployedBytecode.object,'hex')).digest('hex'),settings},null,2)+'\n');
