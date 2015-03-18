######## Default backend parameters:

#archives = r_file, 
getDefaultBackend=function(){
  z=list(hadoop = list(D = "mapred.compress.map.output=true", 
    D = "mapred.map.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec", 
    D = "mapred.output.compress=true", 
    D = "mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec"));
  return(z);
}

######## Determine the set of tags:
getTags = function(input, pattern = ",", backend.parameters=list()){
  
  getTags.map = function(., block) {
    block[,1]<-as.character(block[,1])
    val<-unique(block[,1]);
    keys <- vector(mode="numeric", length=length(val))+1;
    keyval(keys,val)
  }
  
  
  getTags.reduce = function(index, value) {
    z<-unique(value);
    keyval(index,z);
  }
  
  outFile=mapreduce(
    backend.parameters = backend.parameters,
    input = input ,
    input.format = make.input.format("csv",sep=","),
    map = getTags.map,
    reduce = getTags.reduce,
    combine = TRUE
  );
  tags=sort(values(from.dfs(outFile)));
  return(tags);
}

######## Create Numeric Vectors:

createSparseVectors<-function(value,D){
  x<-unlist(strsplit(value,":"));
  ind<-seq(1,length(x),2);
  temp<-as.numeric(substr(x[ind],4,sapply(x[ind],nchar)));
  val<-as.numeric(x[ind+1]);
  
  # create a sparse vector:
  ind2<-rep(1,length(ind));
  sm<-spMatrix(1,D,ind2,temp,x=val);
  return(sm);
}

createDenseVectors<-function(value,D){
  x<-unlist(strsplit(value,":"));
  ind<-seq(1,length(x),2);
  temp<-as.numeric(substr(x[ind],4,sapply(x[ind],nchar)));
  val<-as.numeric(x[ind+1]);
  
  # create dense vector:
  v<-vector('numeric',D)
  v[temp]<-val;
  return(as.numeric(v));
}


createDenseVectorsFromTags<-function(value,tags){
  x<-unlist(strsplit(value,":"));
  ind<-seq(1,length(x),2);
  temp<-match(x[ind],tags);
  val<-as.numeric(x[ind+1]);
  
  # create dense vector:
  v<-vector('numeric',length(tags));
  v[temp]<-val;
  return(as.numeric(v));
}



# create vectors for each time-stamp -  a map/reduce functon
createVectors = function(input, tags, pattern = ",",output.format="native", output=tempfile(),backend.parameters=list()){
  
  createVectors.map = function(., block) {    
    block[,1]<-as.character(block[,1])
    
    keys=as.character(block[,3])
    vals=paste(block[,1],block[,2],sep=":")
    keyval(keys,vals)
  }
  
  
  createVectors.reduce = function(index, value) {
    #write(paste("createVectors.reduce",  Sys.time(), '---->', index, length(value)), stderr());
    
    z<-paste(value,collapse=' ');
    l<-unlist(strsplit(z,' '));
    #v<-t(matrix(createDenseVectors(l,D)));
    v<-t(matrix(createDenseVectorsFromTags(l,tags)));
    kv=keyval(index,v);
    
    #write(paste("createVectors.reduce",  Sys.time(), '<----', index, length(value)), stderr());
    return(kv);
  }
  
  mapreduce(
    backend.parameters = backend.parameters,
    input = input ,
    input.format = make.input.format("csv",sep=","),
    output.format=output.format,
    output=output,
    map = createVectors.map,
    reduce = createVectors.reduce
  )
}

######## Compute Statistics on Numeric Vectors:

computeStatistics = function(input, keyColumn=0, input.format="native", backend.parameters=list()){
  computeStatistics.map = function(keys, X) {    
    if(keyColumn>0){
      X=X[,-keyColumn];
    }
    out=list();
    out$n=dim(X)[1];
    out$mean=colMeans(X);
    out$mss=apply(X,2,function(x) sum((x*x)/out$n));
    out$min=apply(X,2,min);
    out$max=apply(X,2,max);
    out$std=sqrt(out$mss-out$mean*out$mean);
    keyval(1,list(out));
  }   
  
  computeStatistics.reduce = function(., v) {  
    out=list();
    out$n=sum(sapply(v,function(x) (x$n[1])));
    out$min=apply(sapply(v,function(x) (x$min)),1,min);
    out$max=apply(sapply(v,function(x) (x$max)),1,max);   
    out$mean=apply(sapply(v,function(x) ((x$n/out$n)*x$mean)),1,sum);
    out$mss=apply(sapply(v,function(x) ((x$n/out$n)*x$mss)),1,sum);
    out$std=sqrt(out$mss-out$mean*out$mean);
    keyval(1,list(out));   
  }
  
  outFile=mapreduce(
    backend.parameters = backend.parameters,
    input = input ,
    input.format=input.format,
    map = computeStatistics.map,
    reduce = computeStatistics.reduce,
    combine = TRUE
  );
  stats=values(from.dfs(outFile))[[1]];
  return(stats);
}


### find outliers: values outside the [min,max] bounds
applyStats = function(input, stats, tags,  keyColumn=0, input.format="native",output=tempfile(), backend.parameters=list()){
  
  applyStats.map = function(ks, X) {   
    if(keyColumn>0){
      X=X[,-keyColumn];
    }
    
    ind=which(X<stats$min | X>stats$max,arr.ind=TRUE);
    keys=ks[ind[,1]];
    vals=paste(tags[ind[,2]],as.numeric(X[ind]),sep="=");
    keyval(keys,vals);
  }   
  
  mapreduce(
    backend.parameters = backend.parameters,
    input = input ,
    input.format =  input.format,
    output=output,
    map = applyStats.map
  )
}



######## Compute Number of Zero Crossings for each feature:

computeZeroCrossings = function(input,mean, keyColumn=0,input.format="native", backend.parameters=list()){
  computeZeroCrossings.map = function(keys, X) {    
    if(keyColumn>0){
      keys=X[,keyColumn];
      X=X[,-keyColumn];
    }    
    n=dim(X)[1];
    out=list();
    X2=sign(apply(X,2,function(x) (x-mean)));   
    out$zcr=colSums((X2[2:n,]*X2[1:(n-1),])<0);
    out$data=rbind(X2[1,],X2[n,]);
    out$keys=rbind(keys[1],keys[n]);    
    out$n=n;
    keyval(1,list(out));
  }   
  
  computeZeroCrossings.reduce = function(., v) {  
    ks=do.call(rbind,lapply(v,function(x) (x$keys)));
    data=do.call(rbind,lapply(v,function(x) (x$data)));
    zcr=rowSums(sapply(v,function(x) (x$zcr)));
    ns=sum(sapply(v,function(x) (x$n)));
    
    o<-order(ks);
    data=data[o,];
    n=dim(data)[1];
    # compute signs between consecutive points, ie 1->2, 2->3, etc:
    X3=data[2:n,]*data[1:(n-1),];    
    # only count number of crossings from even rows to odds, ie. use even rows:
    zcr=zcr+colSums(X3[seq(2,n-1,2),]<0);    
    keyval(1,zcr/(ns-1));   
  }
  
  outFile=mapreduce(
    backend.parameters = backend.parameters,
    input = input ,
    input.format=input.format,
    map = computeZeroCrossings.map,
    reduce = computeZeroCrossings.reduce,
    combine = FALSE
  );
  zcr=values(from.dfs(outFile))[[1]];
  return(zcr);
}


######## Do K-Means

## euclidean distance between matrices: 

km.dist.fun = function(C, P) {
  apply(
    C,
    1, 
    function(x) 
      colSums((t(P) - x)^2))
}


km.assignToCenters=function(P,centers, keyColumn=0, input.format="native",output=tempfile(), output.format=make.output.format("csv",sep=",",quote=FALSE), backend.parameters=list()){
  
  ## @knitr kmeans.map
  km.assignCenters.map = function(key, P) {
    if(keyColumn>0){
      key=P[,keyColumn];
      P=P[,-keyColumn];
    }    
    P=as.matrix(P);
    D = km.dist.fun(centers, P);
    nearest = max.col(-D);    
    keyval(key,nearest);
  }
  
  mapreduce(
    backend.parameters = backend.parameters,
    input = P ,
    input.format = input.format,
    output =output,
    output.format=output.format,
    map = km.assignCenters.map,
    combine=FALSE
  )  
}


kmeans.mr = function(P, num.clusters, num.iter, keyColumn=0, input.format="native", backend.parameters=list()) {
  
  ## @knitr kmeans.map
  kmeans.map = function(., P) {
    if(keyColumn>0){
      P=P[,-keyColumn];
    }    
    P=as.matrix(P);
    nearest = {
      if(is.null(C)) 
        sample(1:num.clusters, nrow(P), replace = T)
      else {
        D = km.dist.fun(C, P);
        nearest = max.col(-D)
      }
    }    
    keyval(nearest, cbind(1, P))
  }
  
  ## @knitr kmeans.reduce
  kmeans.reduce = function(k, P) {
    write(paste("kmeans.reduce (2)",  Sys.time(), '---->', k, dim(P)), stderr()); 
    kv=keyval(k, t(as.matrix(apply(P, 2, sum))))
    write(paste("kmeans.reduce (2)",  Sys.time(), '<----', k, dim(P)), stderr());
    return(kv);
  }
  
  ## @knitr kmeans-main-1  
  C = NULL
  for(i in 1:num.iter ) {
    write(paste("forLoop.reduce",  Sys.time(), '---->', i), stderr());
    
    C = values(from.dfs(mapreduce(backend.parameters = backend.parameters,P, map = kmeans.map,reduce = kmeans.reduce, combine=TRUE, input.format=input.format)))
    C = C[, -1]/C[, 1]
    if(nrow(C) < num.clusters) {
      C = rbind(C,matrix(rnorm((num.clusters - nrow(C)) * nrow(C)), ncol = nrow(C)) %*% C) 
    }
    
    write(paste("forLoop.reduce",  Sys.time(), '<----', i), stderr());
  }
  C
}
## @knitr end


######## Do Ridge Linear Regression
doLinearRegression=function(input, yIndex,lambda=0, keyColumn=0, input.format="native", backend.parameters=list()){
  linearRegressionHelper.map = function(., Xi) {    
    yi = Xi[,yIndex];    
    if(keyColumn>0){
      Xi=as.matrix(cbind(1,Xi[,cbind(-keyColumn,-yIndex)]));
    }    
    else{
      Xi = as.matrix(cbind(1,Xi[,-yIndex]));
    }
    keyval(1, list(cbind(t(Xi) %*% Xi,t(Xi) %*% yi)));
  }  
  
  outX=values(from.dfs(
    mapreduce(
      backend.parameters = backend.parameters,
      input = input ,
      input.format=input.format,
      map = linearRegressionHelper.map,
      reduce = matrixSum,
      combine = TRUE
    )))[[1]];    
  XtX=outX[,-dim(outX)[2]];
  Xty=outX[,dim(outX)[2]];
  # regularize:
  diag(XtX)=diag(XtX)+lambda;
  beta=solve(XtX, Xty);
  return(beta);
}

# apply linear regression:
applyLinearRegression=function(input, yIndex,beta,keyColumn=0,input.format="native", output=tempfile(),
                               output.format=make.output.format("csv",sep=",",quote=FALSE), backend.parameters=list()){
  applyLinearRegressionHelper.map = function(k, Xi) {
    if(keyColumn>0){
      k=as.character(Xi[,keyColumn]);
      Xi=cbind(1,Xi[,cbind(-keyColumn,-yIndex),]);
    }
    else{
      Xi = cbind(1,Xi[,-yIndex]);
    }
    pred=as.matrix(Xi)%*%beta;
    keyval(k, pred);
  }
  
  mapreduce(
    backend.parameters = backend.parameters,
    input = input ,
    input.format=input.format,
    output=output,
    map = applyLinearRegressionHelper.map,
    output.format=output.format
  )    
}


### do logistic regression: NOTE - regularization does not work, so should always use lambda=0
g = function(z) {z[z>12]=12; z[z< -12]=-12;  out=exp(z)/(1 + exp(z)); return(out);  }
ginv = function(z) log(z/(1-z));
matrixSum = function(., YY) { 
  write(paste("matrixSum",  Sys.time(), '---->', dim(YY)), stderr());
  kv=keyval(1, list(Reduce('+', YY)))
  write(paste("matrixSum",  Sys.time(), '<----', dim(YY)), stderr());
  return(kv);
};

logisticRegression1Iteration=function(input, yIndex, plane,beta0,lambda=0,step=1,keyColumn=0,input.format="native", backend.parameters=list()){
  lr.map = function(., M) {
    Y = M[,yIndex]     
    if(keyColumn>0){
      X=M[,cbind(-keyColumn, -yIndex)];
    }  
    else{
      X = M[,-yIndex];
    }
    X=as.matrix(X);
    # need to compute: 
    # > z=X*beta+W^-1 * (y-p)
    # > beta=(t(X)*W*X)^-1 * t(X)*W*z
    # we compute two parts separately, add them up in reducer
    # and then in main invert part1 and multiply by part2
    scores=X %*% t(plane);
    p=g(scores+beta0);
    W=as.vector(p*(1-p));
    tXW=t(X*W);
    H=tXW%*%X;
    # regularize:
    diag(H)=diag(H)+2*lambda;
    part1=H;
    ###part1inv=qr.solve(part1);
    
    # ridge-normalized by adding lambda terms (v1)
    z=(scores+(step/W)*(Y-p));
    part2=tXW%*%z;
    
    # ridge-normalized by adding lambda terms (v2)
    ###deltaF=t(X)%*%(Y-p);
    ###part2=deltaF-2*lambda*t(plane);    
    
    # return the 2 parts needed to compute the plane
    keyval(1, list(cbind(part1,part2)));    
  }
  
  
  outData = values( from.dfs(
    mapreduce(
      backend.parameters = backend.parameters,
      input=input,
      input.format=input.format,
      map = lr.map,     
      reduce = matrixSum,
      combine = T)
  ))[[1]];  
  part1=outData[,-dim(outData)[2]];
  part2=outData[,dim(outData)[2]];
  newplane= t(solve(part1) %*%part2);  
  ###delta=t(part1inv %*% part2);
  ###newplane2= plane+step*delta; 
  
  return(newplane);  
}


computeLogLikelihoodMR=function(input, yIndex, plane, beta0,keyColumn=0,input.format="native", backend.parameters=list()){
  computeLogLikelihoodMR.map = function(k, M) {
    Y = M[,yIndex]     
    if(keyColumn>0){
      X=M[,cbind(-keyColumn, -yIndex)];
    }  
    else{
      X = M[,-yIndex];
    }
    X=as.matrix(X);
    scores=X %*% t(plane);
    p=g(scores+beta0);
    s1=sum(Y*log(p),na.rm=TRUE);
    s2=sum((1-Y)*log(1-p),na.rm=TRUE);
    keyval(1, s1+s2);
  }
  
  mapreduce(
    backend.parameters = backend.parameters,
    input = input ,
    input.format=input.format,
    map = computeLogLikelihoodMR.map,
    reduce = matrixSum,
    combine=TRUE
  )      
}


logisticRegressionHTF=function(input, yIndex, dims, maxIterations=10, lambda=0, epsilon=0.000001,keyColumn=0, input.format="native", backend.parameters=list()){
  
  getMeanY.map=function(.,M){
    X=as.matrix(M[,yIndex]);
    keyval(1,list(cbind(length(X),sum(X))));
  }
  
  betaData=values(from.dfs(
    mapreduce(
      backend.parameters = backend.parameters,
      input,
      input.format=input.format,
      map = getMeanY.map,     
      reduce = matrixSum,
      combine = T)
  ))[[1]];  
  beta0=ginv(betaData[2]/betaData[1]);
  print(beta0);
  plane = t(rep(0, dims));
  norm=0;
  llscore=values(from.dfs(computeLogLikelihoodMR(input,yIndex,plane, beta0,keyColumn=keyColumn,input.format=input.format)))[[1]];
  for (i in 1:maxIterations) { 
    step=1;
    newplane=logisticRegression1Iteration(input,yIndex,plane,beta0,lambda=lambda,step=step,keyColumn=keyColumn,input.format=input.format)
    newnorm=sum(newplane*newplane); 
    newllscore=values(from.dfs(computeLogLikelihoodMR(input,yIndex,newplane, beta0,keyColumn=keyColumn, input.format=input.format)))[[1]]+lambda*newnorm;
    print(i);
    print(newplane);
    print(newllscore);
    while(newllscore<llscore-epsilon && abs((newllscore-llscore)/newllscore) > epsilon){ 
      step=step/2;
      newplane=logisticRegression1Iteration(input,yIndex,plane,beta0,lambda=lambda,step=step,keyColumn=keyColumn,input.format=input.format)
      newnorm=sum(newplane*newplane); 
      newllscore=values(from.dfs(computeLogLikelihoodMR(input,yIndex,newplane, beta0,keyColumn=keyColumn,input.format=input.format)))[[1]]+lambda*newnorm;
      print(newplane);
      print(newllscore);
      print(step);
    }
    if(abs((newllscore-llscore)/newllscore)< epsilon || newllscore-llscore<epsilon){print("Break"); break;}
    llscore=newllscore; 
    plane=newplane;
  }
  return(c(beta0,plane));
}


### apply logistic regression:
applyLogisticRegression=function(input, yIndex,beta, keyColumn=0, input.format="native", output=tempfile(),
                                 output.format=make.output.format("csv",sep=",",quote=FALSE), backend.parameters=list()){
  applyLogisticRegressionHelper.map = function(k, Xi) {
    if(keyColumn>0){
      k=as.character(Xi[,keyColumn]);
      Xi=cbind(1,Xi[,cbind(-keyColumn,-yIndex),]);
    }
    else{
      Xi = cbind(1,Xi[,-yIndex]);
    }    
    scores=as.matrix(Xi) %*% beta;
    ps=g(scores);
    keyval(k, ps);
  }
  
  mapreduce(
    backend.parameters = backend.parameters,
    input = input ,
    input.format=input.format,
    output=output,
    map = applyLogisticRegressionHelper.map,
    output.format=output.format
  )    
}


