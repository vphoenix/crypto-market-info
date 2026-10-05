// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

interface Token { function balanceOf(address) external view returns(uint256); function approve(address,uint256) external returns(bool); }
interface Folio {
 function mint(uint256,address,uint256) external returns(address[] memory,uint256[] memory);
 function redeem(uint256,address,address[] calldata,uint256[] calldata) external returns(uint256[] memory);
 function bid(uint256,address,address,uint256,uint256,bool,bytes calldata) external returns(uint256);
}
interface Router {
 struct ExactInputParams {bytes path;address recipient;uint256 deadline;uint256 amountIn;uint256 amountOutMinimum;}
 struct ExactOutputParams {bytes path;address recipient;uint256 deadline;uint256 amountOut;uint256 amountInMaximum;}
 function exactInput(ExactInputParams calldata) external payable returns(uint256);
 function exactOutput(ExactOutputParams calldata) external payable returns(uint256);
}

// This runtime is supplied only as an ephemeral eth_call state override.
// It is never deployed, signed, or sent as a transaction by the collector.
contract RouteProbe {
 address constant USDC=0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
 address constant ROUTER=0xE592427A0AEce92De3Edee1F18E0157C05861564;
 struct Plan {
  address folio; uint8 kind; uint256 shares; uint256 minShares;
  address[] assets; bytes[] assetPaths; uint256[] amounts; bytes sharePath;
  uint256 auctionId; address sellToken; address buyToken; uint256 sellAmount; uint256 buyAmount;
 }
 function approve(address token,address spender) internal {
  (bool ok,bytes memory data)=token.call(abi.encodeWithSelector(Token.approve.selector,spender,0));
  require(ok&&(data.length==0||abi.decode(data,(bool))),"approve_zero_failed");
  (ok,data)=token.call(abi.encodeWithSelector(Token.approve.selector,spender,type(uint256).max));
  require(ok&&(data.length==0||abi.decode(data,(bool))),"approve_failed");
 }
 function exactOut(bytes memory path,uint256 amount) internal {
  if(amount==0||path.length==0)return;
  Router(ROUTER).exactOutput(Router.ExactOutputParams(path,address(this),block.timestamp,amount,Token(USDC).balanceOf(address(this))));
 }
 function exactIn(bytes memory path,address token,uint256 amount) internal {
  if(amount==0||path.length==0)return;
  approve(token,ROUTER);
  Router(ROUTER).exactInput(Router.ExactInputParams(path,address(this),block.timestamp,amount,0));
 }
 function execute(Plan calldata p) external returns(uint256 amountIn,uint256 amountOut,uint256 sharesOut,uint256 gasInternal) {
  uint256 gasStart=gasleft(); uint256 initial=Token(USDC).balanceOf(address(this));
  require(initial>0&&p.assets.length==p.assetPaths.length&&p.assets.length==p.amounts.length,"invalid_plan");
  for(uint256 i;i<p.assets.length;i++){if(p.assets[i]!=USDC)require(Token(p.assets[i]).balanceOf(address(this))==0,"preexisting_asset");}
  require(Token(p.folio).balanceOf(address(this))==0,"preexisting_share");
  approve(USDC,ROUTER);
  if(p.kind==0){
   for(uint256 i;i<p.assets.length;i++){exactOut(p.assetPaths[i],p.amounts[i]);approve(p.assets[i],p.folio);}
   Folio(p.folio).mint(p.shares,address(this),p.minShares);
   sharesOut=Token(p.folio).balanceOf(address(this));
   amountIn=initial-Token(USDC).balanceOf(address(this));
   exactIn(p.sharePath,p.folio,sharesOut);
  }else if(p.kind==1){
   exactOut(p.sharePath,p.shares);
   amountIn=initial-Token(USDC).balanceOf(address(this));
   uint256[] memory minimums=new uint256[](p.assets.length);
   Folio(p.folio).redeem(p.shares,address(this),p.assets,minimums);
   for(uint256 i;i<p.assets.length;i++){exactIn(p.assetPaths[i],p.assets[i],Token(p.assets[i]).balanceOf(address(this)));}
   sharesOut=p.shares;
  }else if(p.kind==2){
   require(p.assets.length==2&&p.assets[0]==p.buyToken&&p.assets[1]==p.sellToken,"invalid_bid_assets");
   exactOut(p.assetPaths[0],p.buyAmount);
   approve(p.buyToken,p.folio);
   uint256 beforeBid=Token(USDC).balanceOf(address(this));
   Folio(p.folio).bid(p.auctionId,p.sellToken,p.buyToken,p.sellAmount,p.buyAmount,false,"");
   // A USDC sell leg is already received by the bid, and belongs to output.
   if(p.sellToken==USDC)amountIn=initial-beforeBid;
   else amountIn=initial-Token(USDC).balanceOf(address(this));
   exactIn(p.assetPaths[1],p.sellToken,Token(p.sellToken).balanceOf(address(this)));
  }else{revert("unknown_kind");}
  uint256 finalBalance=Token(USDC).balanceOf(address(this));
  amountOut=finalBalance-(initial-amountIn);
  for(uint256 i;i<p.assets.length;i++){if(p.assets[i]!=USDC)require(Token(p.assets[i]).balanceOf(address(this))==0,"remaining_asset");}
  require(Token(p.folio).balanceOf(address(this))==0,"remaining_share");
  gasInternal=gasStart-gasleft();
 }
}
