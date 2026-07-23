package com.food.service.impl;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import com.food.entity.FoodEntity;
import com.food.io.FoodRequest;
import com.food.io.FoodResponse;
import com.food.repository.FoodRepository;
import com.food.service.FoodService;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@Service

public class FoodServiceImpl implements FoodService {
	@Autowired
	// this is bean which is return by a method define in AWS configuration
	private S3Client s3Client;

	@Autowired
	private FoodRepository foodRepository;

	@Value("${aws.s3.bucket}")
	private String bucketName;

	@Override
	// this method is responsible for uploading images on s3 bucket
	public String uploadFile(MultipartFile file) {
		String fileNameExtension = file.getOriginalFilename()
				.substring(file.getOriginalFilename().lastIndexOf(".") + 1);
		String key = UUID.randomUUID().toString() + "." + fileNameExtension;

		try {
			PutObjectRequest putObjectRequest = PutObjectRequest.builder()
					.bucket(bucketName)
					.key(key)
					.acl(ObjectCannedACL.PUBLIC_READ)
					.contentType(file.getContentType())
					.build();
			PutObjectResponse response = s3Client.putObject(putObjectRequest, RequestBody.fromBytes(file.getBytes()));
			if (response.sdkHttpResponse().isSuccessful()) {
				return "https://" + bucketName + ".s3.amazonaws.com/" + key;
			} else {
				throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "File upload failed");

			}
		} catch (IOException ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
					"An Error accured while uploading the file");

		}

	}

	@Override
	// This method is call by admin side
	public FoodResponse addFood(FoodRequest request, MultipartFile file) {

		FoodEntity foodEntity = convertToEntity(request);
		String imageUrl = uploadFile(file);
		foodEntity.setImageUrl(imageUrl);
		foodEntity.setImageUrl(imageUrl);
		foodEntity = foodRepository.save(foodEntity);
		return convertToResponse(foodEntity);
	}

	@Override
	public List<FoodResponse> readFoods() {
		List<FoodEntity> foodEntities = foodRepository.findAll();
		return foodEntities.stream().map(object -> convertToResponse(object)).collect(Collectors.toList());

		/*
		 * List<FoodResponse> foodResponses=new ArrayList();
		 * foodEntities.forEach(e->{
		 * foodResponses.add(convertToResponse(e));
		 * });
		 * return foodResponses;
		 */
	}

	@Override
	public FoodResponse getFoodById(String id) {
		FoodEntity foodEntity = foodRepository.findById(id).orElse(null);
		if (foodEntity == null) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Food not found for id:" + id);
		}
		return convertToResponse(foodEntity);
	}

	@Override
	public FoodResponse deleteFood(String id) {
		FoodResponse food = getFoodById(id);
		String imageUrl = food.getImageUrl();
		String fileName = imageUrl.substring(imageUrl.lastIndexOf("/") + 1);

		if (deleteFile(fileName)) {
			foodRepository.deleteById(food.getId());
		}
		return food;

	}

	@Override
	public boolean deleteFile(String fileName) {
		DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
				.bucket(bucketName)
				.key(fileName)
				.build();
		s3Client.deleteObject(deleteObjectRequest);
		return true;

	}

	private FoodEntity convertToEntity(FoodRequest request) {
		return FoodEntity.builder()
				.name(request.getName())
				.description(request.getCategory())
				.category(request.getCategory())
				.price(request.getPrice())
				.build();
	}

	private FoodResponse convertToResponse(FoodEntity foodEntity) {
		return FoodResponse.builder()
				.id(foodEntity.getId())
				.name(foodEntity.getName())
				.description(foodEntity.getDescription())
				.category(foodEntity.getCategory())
				.price(foodEntity.getPrice())
				.imageUrl(foodEntity.getImageUrl())
				.build();
	}

}
